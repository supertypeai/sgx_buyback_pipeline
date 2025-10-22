from src.config.settings import LOGGER
from src.fetch_sgx_filings.utils.constants import (
    ACQUISITION_OPTIONS, DISPOSAL_OPTIONS, 
    OTHER_OPTIONS, TYPE_SECURITIES_OPTIONS, 
)

import re
import pdfplumber
import fitz 


def get_all_text_blocks(text_dict: dict[str, any]) -> list[dict[str, any]]:
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
    all_text_blocks: list[dict[str, any]],
    drawings: list[dict[str, any]], 
    options_dict: dict[str, str],
    y_start: float,
    y_end: float
) -> dict[str, bool | None]:
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
    all_text_blocks: list[dict[str, any]],
    drawings: list[dict[str, any]],
    y_start: float,
    y_end: float,
    pattern: str
) -> dict[str, any]:
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
        
        if (abs(d_rect.y0 - others_block['y0']) < 15 and
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


def convert_pdfplumber_bbox_to_fitz(bbox: tuple, page_height: float) -> fitz.Rect:
    x0, y0, x1, y1 = bbox
    # Convert Y coordinates (flip vertically)
    fitz_y0 = page_height - y1
    fitz_y1 = page_height - y0
    return fitz.Rect(x0, fitz_y0, x1, fitz_y1)


def find_section_header(
    all_text_blocks: list[dict[str, any]], 
    bbox_fitz: fitz.Rect
) -> dict[str, any] | None:
    try:
        for block in all_text_blocks:
            if re.search(r"Circumstance\s+giving\s+rise\s+to\s+the\s+interest", block["text"], re.IGNORECASE):
                # Allow 50pt tolerance
                if block['y0'] >= bbox_fitz.y0 - 50:  
                    return block
        return None
    except Exception as error:
        LOGGER.error(f"Error finding section header: {error}", exc_info=True)
        return None
    

def adjust_block_coordinates(
    blocks: list[dict[str, any]], 
    page_offset: float
) -> list[dict[str, any]]:
    try:
        adjusted_blocks = []
        
        for block in blocks:
            adjusted_block = block.copy()
            adjusted_block['y0'] += page_offset
            adjusted_block['y1'] += page_offset
            adjusted_blocks.append(adjusted_block)
        return adjusted_blocks
    
    except Exception as error:
        LOGGER.error(f"[adjust_block_coordinates] Error: {error}")
        return blocks


def adjust_drawing_coordinates(
    drawings: list[dict[str, any]], 
    page_offset: float
) -> list[dict[str, any]]:
    try:
        adjusted_drawings = []

        for drawing in drawings:
            adjusted_drawing = drawing.copy()
            adjusted_drawing['rect'] = fitz.Rect(
                drawing['rect'].x0,
                drawing['rect'].y0 + page_offset,
                drawing['rect'].x1,
                drawing['rect'].y1 + page_offset
            )
            adjusted_drawings.append(adjusted_drawing)
        return adjusted_drawings
    
    except Exception as error:
        LOGGER.error(f"[adjust_drawing_coordinates] Error: {error}")
        return drawings
    

def gather_page_content(
    doc_fitz: fitz.Document,
    start_page_index: int,
    max_pages: int = 3
) -> tuple[list[dict[str, any]], list[dict[str, any]]]:
    try:
        combined_text_blocks = []
        combined_drawings = []
        
        for page_idx in range(start_page_index, min(start_page_index + max_pages, len(doc_fitz))):
            page = doc_fitz.load_page(page_idx)
            text_dict = page.get_text("dict")
            blocks = get_all_text_blocks(text_dict)
            drawings = page.get_drawings()
            
            if page_idx > start_page_index:
                page_offset = sum([doc_fitz.load_page(index).rect.height 
                                 for index in range(start_page_index, page_idx)])
                blocks = adjust_block_coordinates(blocks, page_offset)
                drawings = adjust_drawing_coordinates(drawings, page_offset)
            
            combined_text_blocks.extend(blocks)
            combined_drawings.extend(drawings)
            
        return combined_text_blocks, combined_drawings
    
    except Exception as error:
        LOGGER.error(f"Error gathering page content: {error}", exc_info=True)
        return [], []
    

def find_subsection_blocks(
    combined_text_blocks: list[dict[str, any]], 
    search_start: float
) -> tuple[dict, dict, dict, dict]:
    try:
        acquisition_block = None
        disposal_block = None
        other_circumstances_block = None
        others_specify_block = None
        
        for block in combined_text_blocks:
            if block["y0"] >= search_start:
                if re.search(r"^Acquisition\s+of\s*:\s*$", block["text"], re.IGNORECASE):
                    acquisition_block = block
                elif re.search(r"^Disposal\s+of\s*:\s*$", block["text"], re.IGNORECASE):
                    disposal_block = block
                elif re.search(r"^Other\s+circumstances\s*:\s*$", block["text"], re.IGNORECASE):
                    other_circumstances_block = block
                elif re.search(r"Others\s*\(\s*please\s+specify\s*\)", block["text"], re.IGNORECASE):
                    others_specify_block = block
                    
        return acquisition_block, disposal_block, other_circumstances_block, others_specify_block
    
    except Exception as error:
        LOGGER.error(f"[find_subsection_blocks] Error: {error}")
        return None, None, None, None


def calculate_section_ranges(
    blocks: tuple[dict[str, any] | None, ...],
) -> tuple[tuple[float, float], ...]:
    try:
        acquisition_block, disposal_block, other_circumstances_block, others_specify_block = blocks
        
        acquisition_range = (
            acquisition_block["y1"],
            disposal_block["y0"] if disposal_block else (acquisition_block["y1"] + 150)
        )
        
        disposal_range = (
            disposal_block["y1"] if disposal_block else None,
            other_circumstances_block["y0"] if other_circumstances_block 
            else (disposal_block["y1"] + 100 if disposal_block else None)
        )
        
        other_circumstances_range = (
            other_circumstances_block["y1"] if other_circumstances_block else None,
            others_specify_block["y0"] if others_specify_block 
            else (other_circumstances_block["y1"] + 120 if other_circumstances_block else None)
        )
        
        others_specify_range = (
            others_specify_block["y0"] if others_specify_block else None,
            (others_specify_block["y0"] + 200) if others_specify_block else None
        )
        
        return acquisition_range, disposal_range, other_circumstances_range, others_specify_range
    
    except Exception as error:
        LOGGER.error(f"[calculate_section_ranges] Error: {error}", exc_info=True)
        return ((0,0), (None,None), (None,None), (None,None))


def extract_circumstance_interest_checkbox(
    doc_fitz: fitz.Document, 
    page_number: int, 
    bbox_pdfplumber: tuple
) -> dict[str, any] | None:
    try:
        for page_index in range(page_number, min(page_number + 3, len(doc_fitz))):
            page = doc_fitz.load_page(page_index)
            page_height = page.rect.height
            bbox_fitz = convert_pdfplumber_bbox_to_fitz(bbox_pdfplumber, page_height)
            
            text_dict = page.get_text("dict")
            all_text_blocks_unfiltered = get_all_text_blocks(text_dict)
            
            # Find section header
            section_block = find_section_header(all_text_blocks_unfiltered, bbox_fitz)
            if not section_block:
                continue
            
            print(f'\nSection found on page {page_index}')
            
            # Collect text blocks from current page and next pages
            combined_text_blocks, combined_drawings = gather_page_content(doc_fitz, page_index)
            
            # Search for subsection headers in combined blocks
            subsection_blocks = find_subsection_blocks(combined_text_blocks, section_block["y1"])
            if not subsection_blocks:
                continue
            
            # Define Y-ranges 
            acquisition_range, disposal_range, other_circumstances_range, others_specify_range = \
                calculate_section_ranges(subsection_blocks)
            
            # Initialize results
            results = {
                "acquisition": {},
                "disposal": {},
                "other_circumstances": {},
                "others_specify": {"checked": False, "description": None}
            }
            
            # Extract from combined blocks
            results["acquisition"] = find_options_in_range(
                combined_text_blocks, combined_drawings, 
                ACQUISITION_OPTIONS, *acquisition_range
            )
            
            if disposal_range[0]:
                results["disposal"] = find_options_in_range(
                    combined_text_blocks, combined_drawings,
                    DISPOSAL_OPTIONS, *disposal_range
                )
            
            if other_circumstances_range[0]:
                results["other_circumstances"] = find_options_in_range(
                    combined_text_blocks, combined_drawings, OTHER_OPTIONS, 
                    *other_circumstances_range
                )

                results["other_circumstances"]["Corporate action by Listed Issuer"] = extract_others_description(
                    combined_text_blocks, combined_drawings,
                    *other_circumstances_range,
                    r"Corporate action.*Listed Issuer.*please specify"
                )
            
            if others_specify_range[0]:
                results["others_specify"] = extract_others_description(
                    combined_text_blocks, combined_drawings,
                    *others_specify_range,
                    r"Others\s*\(\s*please specify\s*\)"
                )
            
            # Return if we found any data
            if subsection_blocks[0]:
                return {
                    'page': page_index + 1,
                    'results': results
                }
        
        return None

    except Exception as error:
        LOGGER.error(f"[extract_circumstance_interest_checkbox] Error: {error}", exc_info=True)
        return None


def extract_checkbox_fallback(
    doc: fitz.Document,
    section_pattern: str
) -> dict[str, any] | None:
    try:
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
            subsection_blocks = find_subsection_blocks(all_text_blocks, section_block["y1"])
            if not subsection_blocks:
                continue
            
            # Define Y-ranges for each subsection
            acquisition_range, disposal_range, other_circumstances_range, others_specify_range = \
                calculate_section_ranges(subsection_blocks)
            
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
                all_text_blocks, drawings, ACQUISITION_OPTIONS, *acquisition_range
            )
            
            if disposal_range[0]:
                results["disposal"] = find_options_in_range(
                    all_text_blocks, drawings, DISPOSAL_OPTIONS, *disposal_range
                )
            
            if other_circumstances_range[0]:
                results["other_circumstances"] = find_options_in_range(
                    all_text_blocks, drawings, OTHER_OPTIONS, 
                    *other_circumstances_range
                )

                results["other_circumstances"]["Corporate action by Listed Issuer"] = extract_others_description(
                    all_text_blocks, drawings, *other_circumstances_range,
                    r"Corporate action.*Listed Issuer.*please specify"
                )
            
            if others_specify_range[0]:
                results["others_specify"] = extract_others_description(
                    all_text_blocks, drawings, *others_specify_range,
                    r"Others\s*\(\s*please specify\s*\)"
                )
            
            return {
                'page': page_num + 1,
                'results': results
            }
        
        return None

    except Exception as error:
        LOGGER.error(f"[extract_checkbox_fallback] Error: {error}", exc_info=True)
        return None


def extract_type_securities_checkbox(
    doc: fitz.Document,
    section_pattern: str,
    search_range: int = 150
) -> dict[str, any]:
    for page_num in range(len(doc)):
        try:
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
            
        except Exception as error:
                LOGGER.warning(f"[extract_type_securities_checkbox] Error: {page_num}: {error}")
                continue
        
    return None


def extract_share_tables(
    pdf_object: pdfplumber.PDF, 
    page_number: int, 
    bbox: tuple
) -> list[list[str]]:
    try:
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
            if table and contains_share_rule(table):
                matching_tables.append(item)
                
        if matching_tables:
            merged = merge_tables(matching_tables)
            return merged
            
        return None

    except Exception as error:
        LOGGER.error(f"[extract_share_tables] Error: {str(error)}", exc_info=True)
        return None


def contains_share_rule(table: list[list[str]]) -> bool:
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


def merge_tables(table_items: list[dict[str, any]]) -> list[list[str]]:
    if not table_items:
        return []
    
    merged = table_items[0]['table']
    
    for index in range(1, len(table_items)):
        current = table_items[index]['table']
        prev_page = table_items[index-1]['page']
        curr_page = table_items[index]['page']
        
        # print(f"Pages: {prev_page} -> {curr_page}")
        # print(f"First row: {current[0] if current else 'empty'}")
        
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


def get_shareholder_name(name_patterns: list[str], section_text: str) -> str:
    name = None 

    for pattern in name_patterns:
        match = re.search(pattern, section_text, re.IGNORECASE)
        if match:
            potential_name = match.group(1).strip()
            
            # Remove parenthetical abbreviations like ("FCAML") and trailing
            potential_name = re.sub(r'\s*\([^)]*\)\s*', '', potential_name).strip()
            potential_name = potential_name.rstrip('.')
            
            if potential_name and potential_name != ':' and not potential_name.startswith('('):
                name = potential_name
                break
    
    return name 


def extract_shareholder_name(pdf_object: pdfplumber.PDF, page_number: int, bbox: tuple) -> str | None:
    try:
        page = pdf_object.pages[page_number]
       
        cropped_view = page.crop(bbox)
        section_text = cropped_view.extract_text(x_tolerance=2)
        
        if not section_text:
            LOGGER.warning(f"[extract_shareholder_name] Empty section text on page {page_number}")
            return None 

        # Search for standard headers within the section 
        name_patterns = [
            r"Name of Substantial Shareholder/Unitholder:\s*([^\n]+)",
            r"Name of Director/CEO:\s*([^\n]+)",
            r"(?:\d+\.\s*)?Name of Trustee-Manager(?:/Responsible Person)?:\s*([^\n]+)"
        ]

        # Search full current page first
        full_page_text = page.extract_text(x_tolerance=2)
        if full_page_text:
            name = get_shareholder_name(name_patterns, full_page_text)
            if name:
                return name
        
        # Fallback two previous pages
        if not name:
            for prev_page_idx in range(page_number - 1, page_number - 3, -1):
            
                if prev_page_idx < 0: 
                    continue

                prev_page = pdf_object.pages[prev_page_idx]
                prev_page_text = prev_page.extract_text(x_tolerance=2)

                # print(f'\nprev_text fallback 2 pages: {prev_page_text}\n')
                if prev_page_text:
                    name = get_shareholder_name(name_patterns, prev_page_text)
                    if name:
                        break 

        # Fallback to the earlier pages 
        if not name: 
            for prev_page_idx in range(page_number - 1, 0, -1):
                if prev_page_idx < 0: 
                    continue

                prev_page = pdf_object.pages[prev_page_idx]
                prev_page_text = prev_page.extract_text(x_tolerance=2)

                # print(f'\nprev_text fallback all earlier pages: {prev_page_text}\n')
                if prev_page_text:
                    name = get_shareholder_name(name_patterns, prev_page_text)
                    if name:
                        break 
        return name 
    
    except Exception as error:
        LOGGER.error(f'[extract_shareholder_name] Error: {error}', exc_info=True) 
        return None 


def find_shareholder_sections(pdf_object: pdfplumber.PDF) -> list[dict]:
    primary_anchors = [
        "Quantum of interests in securities held by Trustee-Manager",
        "Name of Substantial Shareholder/Unitholder:",
        "Part II - Substantial Shareholder/Unitholder and Transaction(s) Details",
        "Name of Director/CEO:"
    ]
    
    # Regex to find "Transaction A", "Transaction B", etc
    transaction_anchor_pattern = re.compile(r"^Transaction ?[A-Z]$", re.MULTILINE)

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
    