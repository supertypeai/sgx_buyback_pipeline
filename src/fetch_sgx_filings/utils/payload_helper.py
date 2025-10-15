from bs4 import BeautifulSoup 

from src.utils.sgx_parser_helper import safe_convert_float
from src.fetch_sgx_filings.utils.converter_helper import get_latest_currency, calculate_currency_to_sgd
from src.config.settings import LOGGER
from src.fetch_sgx_filings.utils.constants import (
    ACQUISITION_OPTIONS, DISPOSAL_OPTIONS, 
    OTHER_OPTIONS, TYPE_SECURITIES_OPTIONS, 
    OTHER_CIRCUMSTANCES_RULES, TRANSACTION_KEYWORDS
)

import re
import pdfplumber
import requests
import io


def extract_section_data(soup: BeautifulSoup, section_title: str) -> dict[str, str | list[str]]:
    section_data = {}
    h2 = soup.find('h2', class_='announcement-group-header', string=section_title)
    if not h2: 
        return section_data
    
    section_div = h2.find_next_sibling('div', class_='announcement-group')
    if not section_div:
        return section_data
    
    dt_tags = section_div.find_all('dt')
    for dt in dt_tags:
        dd = dt.find_next_sibling('dd')
        if dd and not dd.find('table'):
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            if key:
                section_data[key] = value
                
    attachment_links = section_div.find_all('a', class_='announcement-attachment')
    if attachment_links:
        base_url = "https://links.sgx.com"
        urls = [f"{base_url}{link.get('href')}" for link in attachment_links if link.get('href')]
        if urls:
            section_data['attachments'] = urls

    return section_data


def get_all_text_blocks(text_dict):
    all_text_blocks = []

    for block in text_dict["blocks"]:
        # text block
        if block["type"] == 0:  
            bbox = block["bbox"]
            text = ""
            for line in block["lines"]:
                for span in line["spans"]:
                    text += span["text"] + " "
            all_text_blocks.append({
                "text": text.strip(),
                "bbox": bbox,
                "y0": bbox[1],
                "y1": bbox[3],
                "x0": bbox[0]
            })
    return all_text_blocks 


def find_options_in_range(
        all_text_blocks, drawings,
        options_dict, y_start, y_end
):
    subsection_results = {}
    
    for option_name, pattern in options_dict.items():
        found = False
        for block in all_text_blocks:
            # Check if block is in the Y-range
            if (y_start <= block["y0"] < y_end and
                re.search(pattern, block["text"], re.IGNORECASE)):
                
                is_checked = False
                
                # Look for checkbox
                for drawing in drawings:
                    d_rect = drawing['rect']
                    
                    if (abs(d_rect.y0 - block['y0']) < 10 and
                        d_rect.x1 <= block['x0'] and
                        drawing['type'] == 'f'):
                        
                        fill_color = drawing.get('fill')
                        
                        if fill_color and fill_color != (1.0, 1.0, 1.0):
                            is_checked = True
                            break
                
                subsection_results[option_name] = is_checked
                found = True
                break
        
        if not found:
            subsection_results[option_name] = None  
    
    return subsection_results


def extract_others_description(
        all_text_blocks, 
        drawings, 
        y_start, y_end,
        pattern: str
):
    result = {
        'checked': False,
        'description': None
    }
    
    # Find "pattern" text
    others_block = None
    for block in all_text_blocks:
        if (y_start <= block["y0"] < y_end and
            re.search(pattern, block["text"], re.IGNORECASE)):
            others_block = block
            break
    
    if not others_block:
        return result
    
    # Check if checkbox is checked
    for drawing in drawings:
        d_rect = drawing['rect']
        
        if (abs(d_rect.y0 - others_block['y0']) < 10 and
            d_rect.x1 <= others_block['x0'] and
            drawing['type'] == 'f'):
            
            fill_color = drawing.get('fill')
            
            if fill_color and fill_color != (1.0, 1.0, 1.0):
                result['checked'] = True
                break
    
    # Extract description text if checked True
    if result['checked'] == True:
        description_y_start = others_block['y1']
        description_y_end = y_end
        
        description_parts = []
        for block in all_text_blocks:
            # Look for text below "pattern"
            if (description_y_start <= block["y0"] < description_y_end and
                # Same or slightly left indent
                block['x0'] >= others_block['x0'] - 20): 
                
                # Skip if it's just empty or very short
                text = block['text'].strip()
                # Ignore very short text
                if len(text) > 3:  
                    description_parts.append(text)
        
        if description_parts:
            result['description'] = ' '.join(description_parts)
    
    return result


def extract_circumstance_interest_checkbox(doc, section_pattern: str) -> dict[str, any] | None:
    for page_num in range(2, len(doc)):
        page = doc.load_page(page_num)
        
        # Extract all text with positions
        text_dict = page.get_text("dict")
        
        # Get all text blocks
        all_text_blocks = get_all_text_blocks(text_dict)
         
        # Find the main section header
        section_block = None
        for block in all_text_blocks:
            if re.search(section_pattern, block["text"], re.IGNORECASE):
                section_block = block
                break
        
        if not section_block:
            continue
        
        # Find subsection headers
        acquisition_block = None
        disposal_block = None
        other_circumstances_block = None
        others_specify_block = None
        
        search_start = section_block["y1"]
        
        for block in all_text_blocks:
            if block["y0"] >= search_start:
                if re.search(r"^Acquisition\s+of\s*:\s*$", block["text"], re.IGNORECASE):
                    acquisition_block = block
                elif re.search(r"^Disposal\s+of\s*:\s*$", block["text"], re.IGNORECASE):
                    disposal_block = block
                elif re.search(r"^Other\s+circumstances\s*:\s*$", block["text"], re.IGNORECASE):
                    other_circumstances_block = block
                elif re.search(r"Others\s*\(\s*please\s+specify\s*\)", block["text"], re.IGNORECASE):
                    others_specify_block = block
        
        if not acquisition_block:
            continue
        
        # Define Y-ranges for each subsection
        acquisition_start = acquisition_block["y1"]
        acquisition_end = disposal_block["y0"] if disposal_block else (acquisition_start + 150)
        
        disposal_start = disposal_block["y1"] if disposal_block else None
        disposal_end = other_circumstances_block["y0"] if other_circumstances_block else (disposal_start + 100 if disposal_start else None)
        
        other_circumstances_start = other_circumstances_block["y1"] if other_circumstances_block else None
        # End at "Others (please specify)" or add buffer
        other_circumstances_end = others_specify_block["y0"] if others_specify_block else (other_circumstances_start + 120 if other_circumstances_start else None)
        
        others_specify_start = others_specify_block["y0"] if others_specify_block else None
        # Look for description within next 200 points
        others_specify_end = others_specify_start + 200 if others_specify_start else None
        
        # Get drawings (checkboxes)
        drawings = page.get_drawings()
        
        results = {
            "acquisition": {},
            "disposal": {},
            "other_circumstances": {},
            "others_specify": {
                "checked": False,
                "description": None
            }
        }
        
        # Extract checkboxes for each subsection
        results["acquisition"] = find_options_in_range(
            all_text_blocks, drawings, ACQUISITION_OPTIONS, acquisition_start, acquisition_end
        )
        
        if disposal_start:
            results["disposal"] = find_options_in_range(
                all_text_blocks, drawings, DISPOSAL_OPTIONS, disposal_start, disposal_end
            )
        
        if other_circumstances_start:
            results["other_circumstances"] = find_options_in_range(
                all_text_blocks, drawings, OTHER_OPTIONS, 
                other_circumstances_start, other_circumstances_end
            )

            results["other_circumstances"]["Corporate action by Listed Issuer"] = extract_others_description(
                all_text_blocks, drawings, other_circumstances_start, other_circumstances_end,
                r"Corporate action.*Listed Issuer.*please specify"
            )
        
        if others_specify_start:
            results["others_specify"] = extract_others_description(
                all_text_blocks, drawings, others_specify_start, others_specify_end,
                r"Others\s*\(\s*please specify\s*\)"
            )
        
        return {
            'page': page_num + 1,
            'results': results
        }
    
    return None


def extract_type_securities_checkbox(doc, section_pattern: str, search_range: int = 150) -> dict[str, any]:
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        
        # Extract all text with positions
        text_dict = page.get_text("dict")
        
        # Get all text blocks
        all_text_blocks = get_all_text_blocks(text_dict)
        
        # Find the section header
        section_block = None
        for block in all_text_blocks:
            if re.search(section_pattern, block["text"], re.IGNORECASE | re.DOTALL):
                section_block = block
                break
        
        if not section_block:
            continue  # Try next page
        
        # Define search area below the header
        search_y_start = section_block["y1"]
        search_y_end = search_y_start + search_range
        
        # Get drawings (checkboxes)
        drawings = page.get_drawings()
        
        results = {}
        
        # Find each option
        for option_name, pattern in TYPE_SECURITIES_OPTIONS.items():
            for block in all_text_blocks:
                # Check if block is in search area and matches pattern
                if (block["y0"] >= search_y_start and 
                    block["y0"] <= search_y_end and
                    re.search(pattern, block["text"], re.IGNORECASE)):
                    
                    is_checked = False
                    
                    # Look for checkbox near this text (within ±10 points Y, to the left)
                    for drawing in drawings:
                        d_rect = drawing['rect']
                        
                        # Check if drawing is on same line and to the left of text
                        if (abs(d_rect.y0 - block['y0']) < 10 and
                            d_rect.x1 <= block['x0'] and
                            drawing['type'] == 'f'):
                            
                            fill_color = drawing.get('fill')
                            
                            # CHECKED = Black/dark fill, UNCHECKED = White fill
                            if fill_color and fill_color != (1.0, 1.0, 1.0):
                                is_checked = True
                                break
                    
                    results[option_name] = is_checked
                    break
        
        if results:
            return {
                'page': page_num + 1,
                'results': results
            }
    
    return None


def build_price_per_share(raw_value: str, number_of_stock: str) -> float | None:
    if raw_value is None or number_of_stock is None:
        return None
    
    try:
        if 'share' in raw_value.lower().strip():
            return safe_convert_float(raw_value)
        
        value = safe_convert_float(raw_value)
        
        price_per_share = None
        if value and number_of_stock:
            price_per_share = round(value / number_of_stock, 4)
            return price_per_share
    
    except Exception as error:
        return LOGGER.error(f"[build price per share] Error: {error}")
    

def build_value(raw_value: str, number_of_stock) -> float | None:
    if raw_value is None or number_of_stock is None:
        return None
    
    try:
        clean_value = raw_value.lower().strip()
        
        if 'us'in clean_value:
            sgd_rate = get_latest_currency()
            usd_value = safe_convert_float(raw_value)
            value = calculate_currency_to_sgd(usd_value, sgd_rate)
            
            if 'share' in clean_value:
                value = number_of_stock * value
            return value 
        
        if 'share' in clean_value:
            return number_of_stock * safe_convert_float(raw_value)
        
        return safe_convert_float(raw_value)
    
    except Exception as error:
        return LOGGER.error(f"[build value] Error: {error}")


def get_circumstance_interest(circumstance_interest: dict[str, any]):
    try:
        for key, value in circumstance_interest.items():
            if key == 'others_specify':
                checked = value.get('checked')
                desc = value.get('description')
                if checked:
                    return {
                        'key': 'others_specify',
                        'checked': checked, 
                        'description': desc
                    }
            else:
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, dict) and 'checked' in sub_value:
                        checked = sub_value.get('checked')
                        desc = sub_value.get('desc')
                        if checked: 
                            return {
                                'key': key, 
                                'specific_key': sub_key,
                                'checked': checked,
                                'description': desc
                            }
                    elif sub_value:
                        return {
                            'key': key,
                            'specific_key': sub_key,
                            'checked': sub_value
                        }

    except Exception as error:
        LOGGER.error(
            f"[get_transaction_type] Error: {error}"
        )


def get_transaction_type_from_desc(description: str) -> str:
    try:
        if not description:
            LOGGER.info(f'[get_transaction_type_from_desc] description is None')
            return None 

        desc_lower = description.lower()

        transaction_type = next(
            (key for key, values in TRANSACTION_KEYWORDS.items()
            if any(value.lower() in desc_lower for value in values)),
            None
        )

        if not transaction_type:
            LOGGER.warning(f"[get_transaction_type_from_desc] No keywords matched for description: '{description}'")

        return transaction_type

    except Exception as error:
        LOGGER.error(f"[get_transaction_type_from_desc] Error: {error}")
        return None


def build_transaction_type(circumstance_interest_raw: dict[str, any]) -> str:
    try:
        circumstance_interest = circumstance_interest_raw.get('results')
        circumstance_interest = get_circumstance_interest(circumstance_interest)
        print(f'raw type: {circumstance_interest}')

        transaction_type = None 
        key = circumstance_interest.get('key')
        checked = circumstance_interest.get('checked')
        specific_key = circumstance_interest.get('specific_key')

        if checked:
            if key == 'others_specify':
                description = circumstance_interest.get('description', None)
                transaction_type = get_transaction_type_from_desc(description)
            elif key == 'acquisition':
                transaction_type = 'buy'
            elif key == 'disposal':
                transaction_type = 'sell'
            elif key == 'other_circumstances':
                lookup_key = specific_key.lower().strip()
                transaction_type = OTHER_CIRCUMSTANCES_RULES.get(lookup_key)

        return transaction_type

    except Exception as error:
        LOGGER.error(f"[build_transaction_type] Error: {error}")
        return None
                

def extract_shares_table(pdf_url: str) -> list[list[str]]:
    response = requests.get(pdf_url)
    response.raise_for_status()

    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        all_tables = []
        
        # Extract ALL tables with page info
        for page_num, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            for table in tables:
                all_tables.append({
                    'page': page_num,
                    'table': table
                })
        
        # Filter matching tables
        matching_tables = []
        for item in all_tables:
            table = item['table']
            if table and contains_share_rule(table):
                matching_tables.append(item)
        
        # Debug
        print(f"Found {len(matching_tables)} matching tables")
        for index, item in enumerate(matching_tables):
            print(f"Table {index} on page {item['page']}: {len(item['table'])} rows")
            print(f"First row: {item['table'][0]}")
        
        if matching_tables:
            # Merge tables from consecutive pages
            merged = smart_merge_tables(matching_tables)
            return merged
        
        return []


def contains_share_rule(table):
    table_text = ' '.join([
        ' '.join([str(cell) for cell in row if cell])
        for row in table
    ]).lower()
    
    clean_text = ' '.join(table_text.split())
    
    # Match voting shares/units (with or without "ordinary")
    if 'voting shares/units' in clean_text or 'ordinary voting units' in clean_text:
        # Exclude pure rights/options/warrants tables
        if 'rights/options/warrants held:' in clean_text and 'voting shares/units held' not in clean_text:
            return False
        return True
    
    # Match convertible debentures pattern
    if 'convertible debentures' in clean_text and 'voting shares/units' in clean_text or 'ordinary voting units' in clean_text:
        return True
    
    return False


def smart_merge_tables(table_items):
    if not table_items:
        return []
    
    merged = table_items[0]['table']
    
    for index in range(1, len(table_items)):
        current = table_items[index]['table']
        prev_page = table_items[index-1]['page']
        curr_page = table_items[index]['page']
        
        print(f"\nChecking table {index}:")
        print(f"Pages: {prev_page} -> {curr_page}")
        print(f"First row: {current[0] if current else 'empty'}")
        
        # If on consecutive pages, it's likely a continuation
        if curr_page - prev_page <= 1:
            # Check if it's just percentage rows
            is_percentage_only = all(
                'as a percentage' in ' '.join([str(c) for c in row if c]).lower()
                for row in current
            )
            
            print(f"Is percentage only: {is_percentage_only}")
            
            if is_percentage_only:
                print(f"MERGING")
                merged.extend(current)
                
            # Different header = continuation
            elif current[0] != merged[0]:  
                print(f"MERGING (different structure)")
                merged.extend(current)
    
    return merged




