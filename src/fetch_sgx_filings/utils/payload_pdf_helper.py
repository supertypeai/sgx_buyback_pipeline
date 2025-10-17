import re
import pdfplumber
import requests
import io

from src.config.settings import LOGGER
from src.fetch_sgx_filings.utils.constants import (
    ACQUISITION_OPTIONS, DISPOSAL_OPTIONS, 
    OTHER_OPTIONS, TYPE_SECURITIES_OPTIONS, 
)


def get_all_text_blocks(text_dict):
    all_text_blocks = []

    for block in text_dict["blocks"]:
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
            continue 
        
        # Define search area below the header
        search_y_start = section_block["y1"]
        search_y_end = search_y_start + search_range
        
        # Get drawings (checkboxes)
        drawings = page.get_drawings()
        
        results = {}
        
        # Find each options
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
                            
                            # Checked = Black/dark fill, Unchecked = White fill
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


def extract_share_tables(
    pdf_object: pdfplumber.PDF, 
    page_number: int, 
    bbox: tuple
) -> list[list[str]]:
    found_tables = []

    # Search the initial page within the specified bounding box
    start_page = pdf_object.pages[page_number]
    cropped_view = start_page.crop(bbox)
    initial_tables = cropped_view.extract_tables()

    for table in initial_tables:
        found_tables.append({'page': page_number, 'table': table})

    # Continue searching on the next 3 pages 
    for page_idx in range(page_number + 1, min(len(pdf_object.pages), page_number + 4)):
        next_page = pdf_object.pages[page_idx]
        tables_on_next_page = next_page.extract_tables()
        for table in tables_on_next_page:
            found_tables.append({'page': page_idx, 'table': table})
            
    matching_tables = []
    for item in found_tables:
        table = item['table']
        # LOGGER.info(f"table on loop: {json.dumps(table, indent=2)}")
        if table and contains_share_rule(table):
            matching_tables.append(item)
            
    if matching_tables:
        # LOGGER.info(f"matching tables: {json.dumps(matching_tables, indent=2)}")
        merged = merge_tables(matching_tables)
        # LOGGER.info(f'merged tables: {json.dumps(merged, indent=2)}')
        return merged
        
    return None


def contains_share_rule(table):
    table_text = ' '.join([
        ' '.join([str(cell) for cell in row if cell])
        for row in table
    ]).lower()
    
    clean_text = ' '.join(table_text.split())
    
    if 'rights/options/warrants held' in clean_text or 'rights/options/warrants over' in clean_text:
        # Only exclude if it doesn't also mention voting shares/units
        if 'voting shares/units' not in clean_text and 'ordinary voting units' not in clean_text:
            return False
    
    if ('immediately before' in clean_text or 'immediately after' in clean_text) and \
       'direct interest' in clean_text and 'deemed interest' in clean_text:
        # But only if it's about voting shares, not just rights/options/warrants
        if 'voting shares/units' in clean_text or 'ordinary voting units' in clean_text:
            return True
    
        # Check if it has the rights/options/warrants exclusion keywords
        if 'rights/options/warrants held' in clean_text or 'rights/options/warrants over' in clean_text:
            return False
        return True 
        
    # Match voting shares/units (with or without "ordinary")
    if 'voting shares/units' in clean_text or 'ordinary voting units' in clean_text:
        return True
    
    # Match convertible debentures pattern
    if 'convertible debentures' in clean_text and ('voting shares/units' in clean_text or 'ordinary voting units' in clean_text):
        return True
    
    return False


def merge_tables(table_items):
    if not table_items:
        return []
    
    merged = table_items[0]['table']
    
    for index in range(1, len(table_items)):
        current = table_items[index]['table']
        prev_page = table_items[index-1]['page']
        curr_page = table_items[index]['page']
        
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


def find_shareholder_sections(pdf_object: pdfplumber.PDF) -> list[dict]:
    primary_anchors = [
        "Quantum of interests in securities held by Trustee-Manager",
        "Name of Substantial Shareholder/Unitholder:",
        "Part II - Substantial Shareholder/Unitholder and Transaction(s) Details",
        "Name of Director/CEO:"
    ]
    
    # Regex to find "Transaction A", "Transaction B", etc
    transaction_anchor_pattern = re.compile(r"^Transaction [A-Z]$", re.MULTILINE)

    found_primary = []
    found_transactions = []

    for index, page in enumerate(pdf_object.pages):
        # Find primary anchors
        for anchor_text in primary_anchors:
            found = page.search(anchor_text, case=False)
            for item in found:
                found_primary.append({'text':anchor_text, 'page_number': index, 'top': item['top']})
        
        # Find transaction anchors
        page_text = page.extract_text()
        if page_text:
            for match in transaction_anchor_pattern.finditer(page_text):
                # Find the coordinates of this text match
                bbox = page.search(match.group(0), case=True)
                if bbox:
                    found_transactions.append({'page_number': index, 'top': bbox[0]['top']})

    # Sort all found anchors to process them in order
    found_primary.sort(key=lambda x: (x['page_number'], x['top']))
    found_transactions.sort(key=lambda x: (x['page_number'], x['top']))

    final_anchors = []
    if len(found_primary) > 1 and "Name of Substantial Shareholder/Unitholder:" in [a['text'] for a in found_primary]:
        # Case: Multi-shareholder document. Use the primary anchors
        final_anchors = found_primary
    elif len(found_transactions) > 1:
        # Case: Multi-transaction document. Use the "Transaction A/B" anchors
        final_anchors = found_transactions
    else:
        # Case: Simple single-filer document. Use the single primary anchor found
        final_anchors = found_primary

    # Build the sections based on the chosen final anchors 
    shareholder_sections = []
    if not final_anchors:
        return []
        
    page_width = pdf_object.pages[0].width
    for index, anchor in enumerate(final_anchors):
        page = pdf_object.pages[anchor['page_number']]
        section_top = anchor['top']
        section_bottom = page.height
        
        if index + 1 < len(final_anchors) and final_anchors[index+1]['page_number'] == anchor['page_number']:
            section_bottom = final_anchors[index+1]['top']
        
        section_bbox = (0, section_top, page_width, section_bottom)
        shareholder_sections.append({
            'page_number': anchor['page_number'],
            'bbox': section_bbox
        })

    return shareholder_sections
    