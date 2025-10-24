from bs4 import BeautifulSoup 
from dataclasses import asdict
from io import BytesIO

from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    build_transaction_type, 
    build_price_per_share, 
    build_value,
    safe_convert_float
)
from sgx_scraper.utils.sgx_parser_helper import (
    extract_symbol, 
    matching_symbol,
    safe_convert_datetime
)
from sgx_scraper.fetch_sgx_filings.utils.payload_pdf_helper import (
    extract_circumstance_interest_checkbox,
    extract_type_securities_checkbox,
    extract_share_tables,
    find_shareholder_sections,
    extract_shareholder_name, 
    extract_checkbox_fallback
)
from sgx_scraper.fetch_sgx_filings.utils.payload_html_helper import extract_section_data
from sgx_scraper.fetch_sgx_filings.models import SGXFilings
from sgx_scraper.config.settings import LOGGER

import fitz
import requests
import re 
import json 
import pdfplumber 
import io 


def open_pdf(pdf_url: str) -> fitz.Document:
    if not pdf_url:
        return ''

    try:
        if pdf_url.endswith('.pdf'):
            response = requests.get(pdf_url, timeout=15)
            response.raise_for_status()
    
        # Open PDF from memory
        doc = fitz.open(stream=BytesIO(response.content), filetype="pdf")
        return doc
    
    except requests.exceptions.RequestException as error:
        LOGGER.error(f"[sgx_filings] Failed to download PDF: {error}")
        return None
    
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to open PDF: {error}")
        return None 


def parse_pdf(doc: fitz.Document, end_page: int = None, start_page: int = 2) -> str:
    full_text = ''
    
    if end_page is None: 
        end_page = len(doc)

    for page_num in range(start_page, end_page):
        try:
            page = doc[page_num] 
            # Get text with position info
            blocks = page.get_text("dict")["blocks"]
            
            # Group text by Y position (vertical)
            text_by_position = {}
            
            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        # Y coordinate (rounded)
                        y_pos = round(line["bbox"][1]) 
                        
                        line_text = ""
                        for span in line["spans"]:
                            line_text += span["text"]
                        
                        if line_text.strip():
                            if y_pos not in text_by_position:
                                text_by_position[y_pos] = []
                            text_by_position[y_pos].append(line_text.strip())
            
            # Sort by Y position and reconstruct
            sorted_text = []
            for y in sorted(text_by_position.keys()):
                sorted_text.append(" ".join(text_by_position[y]))
            
            full_text += "\n".join(sorted_text) + "\n"
        
        except Exception as error:
            LOGGER.error(f'[sgx_filings] Error parsing page {page_num}: {error}')
            continue 

    return full_text


def extract_html_content(soup: BeautifulSoup) -> dict[str, str]:
    payload_html = {}

    try:
        issuer_section = extract_section_data(soup, 'Issuer & Securities')
        attachments_section = extract_section_data(soup, 'Attachments')

        # Get symbol
        issuer_security = issuer_section.get('Securities', None)
       
        symbol = extract_symbol(issuer_security)
     
        if not symbol:
            issuer_name = issuer_section.get('Issuer/ Manager')
            symbol = matching_symbol(issuer_name)

        # Get the second attachment 
        attachments = attachments_section.get('attachments', [])
        attachment = attachments[-1] if attachments else None

        payload_html.update({
            'symbol': symbol,
            'url':attachment
        })

    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to extract HTML content: {error}")

    return payload_html


def extract_date(text: str) -> str | None:
    try:
        match = re.search(
            r'Date of acquisition of or change in interest:.*?(\d{2}[-/](?:[A-Za-z]{3}|\d{2})[-/]\d{4})',
            text,
            re.DOTALL
        )
        if match:
            return match.group(1)
        else:
            LOGGER.warning(f"[sgx_filings] Date of acquisition not found")
            return None
        
    except Exception as error:
        print(f"[sgx_filings] Error extracting date: {error}")
        return None


def extract_number_of_stock(text: str) -> str | None:
    try:
        # Pattern that handles the question number appearing before or after
        pattern = r'''
            acquired\ or\s+           # Match "acquired or" followed by whitespace
            (?:\d+\.\s+)?             # Optional: number with period and whitespace (e.g., "1. ")
            disposed\ of\ by\s+       # Match "disposed of by" followed by whitespace
            (?:                       # Non-capturing group for person types
                Director/CEO
                |
                Substantial\ Shareholders?/Unitholders?
                |
                Trustee-Manager/Responsible\ Person
            )
            \s*:\s*                   # Optional whitespace, colon, optional whitespace
            (.+?)                     # Capture group 1: the actual value (non-greedy)
            (?=                       # Positive lookahead (don't consume)
                \n\s*\d+\.            # Either: newline, optional whitespace, number with period
                |                     # Or:
                $                     # End of string
            )
        '''

        match = re.search(pattern, text, re.VERBOSE | re.DOTALL | re.IGNORECASE)
        if match:
            raw_value = match.group(1).strip()
            return raw_value
        
        return None
    
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Error extracting number of stock: {error}")
        return None


def extract_value(text: str) -> str | None:
    try:
        # Match "Amount of consideration...by Director/CEO", "...by Substantial Shareholders",
        # or "...by Trustee-Manager/Responsible Person"
        pattern = r'''
            Amount\ of\ consideration    # Match "Amount of consideration"
            .*?                          # Any characters (non-greedy) until we find...
            by\s+                        # Match "by" followed by whitespace
            (?:                          # Non-capturing group for person types
                Director/CEO
                |
                Substantial\ Shareholders?/Unitholders?
                |
                Trustee-Manager/Responsible\ Person
            )
            [^:]*                        # Any characters except colon (greedy)
            :\s*                         # Colon followed by optional whitespace
            \n\s*                        # Newline followed by optional whitespace
            (.+?)                        # Capture group 1: the actual value (non-greedy)
            (?=                          # Positive lookahead (don't consume)
                \n\s*\d+\.               # Either: newline, optional whitespace, number with period
                |                        # Or:
                \n\n                     # Two newlines (blank line)
                |                        # Or:
                \n[A-Z]                  # Newline followed by uppercase letter
                |                        # Or:
                $                        # End of string
            )
        '''

        match = re.search(pattern, text, re.VERBOSE | re.DOTALL | re.IGNORECASE)
        if match:
            raw_value = match.group(1).strip()
            return raw_value
        
        return None
    
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Error extracting total value: {error}")
        return None


def parse_share_table_values(pdf_object: pdfplumber.PDF, page_number: int, bbox: tuple) -> tuple[dict, dict]:
    try:
        share_tables = extract_share_tables(pdf_object, page_number, bbox)
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to extract tables from PDF: {error}")
        return None, None

    shares_before = {}
    shares_after = {}
    table_values = []
    table_with_values_index = [1, 2, 4, 5]
    
    try:
        for index in range(len(share_tables)):
            if index not in table_with_values_index:
                continue

            table = share_tables[index]
            if len(table) < 4:
                continue
            
            # Get value from "Total" column
            share_value = table[3]  
            table_values.append(share_value)

    except Exception as error:
        LOGGER.error(f"[sgx_filings] Error while parsing tables: {error}")
        return None 
    
    total_share_before = table_values[0] if len(table_values) > 0 else None
    total_share_before_percentage = table_values[1] if len(table_values) > 1 else None
    total_share_after = table_values[2] if len(table_values) > 2 else None
    total_share_after_percentage = table_values[3] if len(table_values) > 3 else None

    shares_before.update({
        'total_shares': total_share_before,
        'percentage': total_share_before_percentage
    })

    shares_after.update({
        'total_shares': total_share_after,
        'percentage': total_share_after_percentage
    })

    return shares_before, shares_after


def build_individual_share_record(pdf_object: pdfplumber.PDF, page_number: int, bbox: tuple) -> dict[str, any]:
    shares_before_raw, shares_after_raw = parse_share_table_values(pdf_object, page_number, bbox)

    if not shares_before_raw and not shares_after_raw:
        return None

    shares_before = safe_convert_float(shares_before_raw.get("total_shares"))
    shares_before_percentage = safe_convert_float(shares_before_raw.get("percentage"))

    shares_after = safe_convert_float(shares_after_raw.get("total_shares"))
    shares_after_percentage = safe_convert_float(shares_after_raw.get("percentage"))

    return {
        "shares_before": shares_before,
        "shares_before_percentage": shares_before_percentage,
        "shares_after": shares_after,
        "shares_after_percentage": shares_after_percentage,
    }


def fallback_extract_transaction_detail(
    page, 
    transaction_date: str
) -> tuple[str, float, float, str]:
    page_text = page.extract_text(x_tolerance=2)
    
    date = transaction_date
    if not date:
        date = safe_convert_datetime(extract_date(page_text))
    
    raw_number_of_stock = extract_number_of_stock(page_text)
    number_of_stock = safe_convert_float(raw_number_of_stock)
    
    raw_value = extract_value(page_text)
    value = build_value(raw_value, number_of_stock)
    
    print(f'\nraw value: {raw_value}, value: {value}, number_of_stock: {raw_number_of_stock}')
    
    return date, number_of_stock, value, raw_value, raw_number_of_stock


def apply_fallback_for_multiple_shareholder(all_records: list[dict], doc_fitz: fitz.Document):
    circumstance_interest_raw = extract_checkbox_fallback(
        doc_fitz, r"Circumstance giving rise to.*?interest"
    )
    fallback_transaction_type = build_transaction_type(circumstance_interest_raw)

    pdf_text = parse_pdf(doc_fitz)

    try:
        for record in all_records:
            transaction_type = record.get('transaction_type')
            number_of_stock = record.get('number_of_stock')
            value = record.get('value')

            if not transaction_type or not number_of_stock or not value:
                # Number of stock
                raw_number_of_stock = extract_number_of_stock(pdf_text)
                number_of_stock = safe_convert_float(raw_number_of_stock)

                # Value
                raw_value = extract_value(pdf_text)
                value = build_value(raw_value, number_of_stock)

                # Price per share
                price_per_share = build_price_per_share(raw_value, number_of_stock)

                record.update({
                    'transaction_type': fallback_transaction_type,
                    'number_of_stock': number_of_stock,
                    'value': value,
                    'price_per_share': price_per_share
                })
    except Exception as error:
        LOGGER.error(f"[apply_fallback_for_multiple_shareholder] Error in fallback for multiple shareholders: {error}")


def extract_symbol_fallback(doc_fits: fitz.Document, start_page: int = 1, end_page: int = 4) -> str:
    pdf_text = parse_pdf(doc_fits, end_page=end_page, start_page=start_page)
    try:
        pattern = r'(?:\d+\.\s*)?Name\s+of\s+Listed\s+Issuer\s*:?\s*(?:\d+\.\s*)?([^\n]+?)(?=\s*\d+\.\s*Type\s+of\s+Listed\s+Issuer|\s*\d+\.|$)'
        
        match = re.search(pattern, pdf_text, re.IGNORECASE | re.DOTALL)
        
        if match:
            name = match.group(1).strip()
            # Remove leading numbers
            name = re.sub(r'^\d+\.\s*', '', name)  
            # Normalize whitespace
            name = re.sub(r'\s+', ' ', name)  
            return name.strip()
        
        return None 
    except Exception as error:
        LOGGER.error(f"[extract_symbol_fallback] Error extracting symbol fallback: {error}")
        return None


def build_special_case_value(raw_value: str, base_record: dict[str, any]) -> tuple[list[dict[str, any]], bool]:
    if not raw_value:
        return [base_record], False

    is_special_case = False 

    multi_transaction_pattern = r"""
        ([\d,]+(?:\.\d+)?)              # Capture number (e.g., "3,844,078")
        \s*
        (?:units?|shares?|              # Match "unit", "units", "share", "shares"
        securit(?:y|ies)|            # "security" or "securities"
        stapled\s+securit(?:y|ies))  # "stapled security/securities"
        \s+at\s+                        # Match " at "
        (?:(?:an?\s+)?issue\s+)?        # Optional "issue" or "a/an issue"
        (?:(?:an?\s+)?price\s+)?        # Optional "a/an price" ← MADE OPTIONAL!
        (?:of\s+)?                      # Optional "of "
        (?:sg\$|s\$|usd|sgd|            # Optional currency symbols
        hkd|us\$|\$)?
        \s*
        ([\d,]+(?:\.\d+)?)              # Capture price (e.g., "2.2242")
        \s*per\s+                       # Match " per "
        (?:unit|share|security|         # Match "unit", "share", etc.
        stapled\s+security)
    """
    
    try:
        matches = re.findall(multi_transaction_pattern, raw_value, re.IGNORECASE | re.VERBOSE)
       
        if len(matches) >= 2:
            LOGGER.info(f"[build_special_case_value] Detected {len(matches)} transactions in: {raw_value}")
            
            new_records = []
            for index, (value, price_per_share) in enumerate(matches):
                copy_record = base_record.copy()
            
                price_per_share = safe_convert_float(price_per_share)
                value = safe_convert_float(value)
                value = value * price_per_share
                value = round(value, 2)

                copy_record.update({
                    'value': value, 
                    'price_per_share': price_per_share
                })

                print(f"Transaction {index+1}: {value}.{price_per_share} = {value}")
                new_records.append(copy_record)
            
            is_special_case = True 
            return new_records, is_special_case
        
        list_all_record =  [base_record]
        return list_all_record, is_special_case

    except Exception as error:
        LOGGER.error(f"[build_special_case_value] Error processing special case value: {error}")
        return [], False 


def build_special_case_multiple_dates(
    raw_number_of_stock: str, 
    raw_value: str, 
    base_record: dict[str, any]
) -> tuple[list[dict[str, any]], bool]:
    if not raw_number_of_stock or not raw_value:
        return [base_record], False 

    is_special_case = False 

    # Pattern to extract number + date from number_of_stock field
    number_date_pattern = r"""
        ([\d,]+(?:\.\d+)?)              # Capture number
        \s+
        (?:shares?|units?|securit(?:y|ies))  # Match share/unit type
        \s+on\s+                        # Match " on "
        (\d{1,2}\s+\w+\s+\d{4})         # Capture date: "7 Nov 2024" format
    """

    price_date_pattern = r"""
        (?:paid\s+)?                    # Optional "paid"
        (?:sg\$|s\$|usd|sgd|\$)?        # Optional currency
        \s*
        ([\d,]+(?:\.\d+)?)              # Capture price
        \s+per\s+
        (?:share|unit|security)         # Match per share/unit
        \s+on\s+                        # Match " on "
        (\d{1,2}\s+\w+\s+\d{4})         # Capture date: "7 Nov 2024" format
    """

    try:
        number_matches = re.findall(number_date_pattern, raw_number_of_stock, re.IGNORECASE | re.VERBOSE)
        price_matches = re.findall(price_date_pattern, raw_value, re.IGNORECASE | re.VERBOSE)

        print(f"DEBUG number_matches: {number_matches}")
        print(f"DEBUG price_matches: {price_matches}")

        if len(number_matches) >= 2 and len(price_matches) >= 2:
            LOGGER.info(f"[build_special_case_multiple_dates] Detected {len(number_matches)} transactions with dates")

            number_by_date = {}
            for number, number_date in number_matches:
                number_date = number_date.strip()
                number_clean = safe_convert_float(number)
                number_by_date[number_date] = number_clean 

            value_by_date = {}
            for value, value_date in price_matches:
                value_date = value_date.strip()
                value_clean = safe_convert_float(value)
                value_by_date[value_date] = value_clean 

            new_records = []
            for number_stock_date in number_by_date.keys():
                if number_stock_date in value_by_date:
                    number_of_stock = number_by_date[number_stock_date]
                    price_per_share = value_by_date[number_stock_date]
                    new_value = number_of_stock * price_per_share
                    new_value = round(new_value, 2)

                    transaction_date = safe_convert_datetime(number_stock_date)
                    
                    copy_record = base_record.copy()
                    copy_record.update({
                        "transaction_date": transaction_date,
                        "number_of_stock": number_of_stock,
                        "price_per_share": price_per_share,
                        "value": new_value,
                    })
                    
                    print(f"Matched: {number_stock_date} → {number_of_stock} x {price_per_share} = {new_value}")
                    new_records.append(copy_record)
            
            is_special_case = True 
            return new_records, is_special_case

        list_all_record =  [base_record]
        return list_all_record, is_special_case

    except Exception as error:
        LOGGER.error(f"[build_special_case_multiple_dates] Error: {error}")
        return [], False 


def extract_transaction_details(
    pdf_object: pdfplumber.PDF, 
    page_number: int, 
    bbox: tuple
) -> list[dict] | None:
    try:
        base_details = {
            "transaction_date": None,
            "number_of_stock": None,
            "value": None,
            "price_per_share": None,
        }
        
        page = pdf_object.pages[page_number]
        cropped_view = page.crop(bbox)
        section_text = cropped_view.extract_text(x_tolerance=2)
        
        if not section_text:
            return base_details

        # Transaction date
        transaction_date = extract_date(section_text)
        transaction_date = safe_convert_datetime(transaction_date)

        # Number of stock
        raw_number_of_stock = extract_number_of_stock(section_text)
        number_of_stock = safe_convert_float(raw_number_of_stock)

        # Value 
        raw_value = extract_value(section_text)
        value = build_value(raw_value, number_of_stock)
        print(f'\nraw value: {raw_value}, value: {value}, number_of_stock: {raw_number_of_stock}')

        # Fallback with next 2 pages 
        if not value and not number_of_stock:
            for page_index in range(page_number, page_number + 3):
                transaction_date, number_of_stock, value, raw_value, raw_number_of_stock = \
                    fallback_extract_transaction_detail(pdf_object.pages[page_index], transaction_date)
                
                if value or number_of_stock:
                    break 
        
        # Fallback for previous page
        if not value and not number_of_stock:
            for page_index in range(page_number -1, page_number - 3, -1):
                transaction_date, number_of_stock, value, raw_value, raw_number_of_stock = \
                    fallback_extract_transaction_detail(pdf_object.pages[page_index], transaction_date)
                
                if value or number_of_stock:
                    break 

        # Price per share 
        price_per_share = build_price_per_share(raw_value, number_of_stock)
    
        base_details.update({
            "transaction_date": transaction_date,
            "number_of_stock": number_of_stock,
            "value": value,
            "price_per_share": price_per_share,
        })

        # Special case for value have two data with price per share 
        details_list, is_special_value = build_special_case_value(raw_value, base_details)
        if len(details_list) > 1: 
            return details_list, is_special_value
        
        # Special case for value have two data with price per share with two date 
        details_list, is_special_value = build_special_case_multiple_dates(
            raw_number_of_stock, 
            raw_value, 
            base_details
        )
        if len(details_list) > 1:
            return details_list, is_special_value
        
        return [base_details], is_special_value
    
    except Exception as error:
        LOGGER.error(f'[sgx_filings] Error: {error}', exc_info=True) 
        return None 


def extract_records(pdf_url: str, doc_fitz) -> list[dict] | None:
    try:
        response = requests.get(pdf_url, timeout=15)
        response.raise_for_status()
        pdf_file = io.BytesIO(response.content)

        with pdfplumber.open(pdf_file) as pdf:
            shareholder_sections = find_shareholder_sections(pdf)
            print(f'raw section: {shareholder_sections}')

            all_records = []
            for shareholder_section in shareholder_sections:
                individual_share_data = build_individual_share_record(
                    pdf,
                    shareholder_section['page_number'],
                    shareholder_section['bbox']
                )

                if not individual_share_data:
                    continue

                # Extract shareholder name 
                shareholder_name = extract_shareholder_name(
                    pdf, shareholder_section['page_number'],
                    shareholder_section['bbox']
                )

                # Extract additional fields 
                transaction_details, is_special_value = extract_transaction_details(
                    pdf,
                    shareholder_section['page_number'],
                    shareholder_section['bbox']
                )
                
                # Extract transaction type
                circumstance_interest_raw = extract_circumstance_interest_checkbox(
                    doc_fitz, 
                    shareholder_section['page_number'],
                    shareholder_section['bbox']
                )
                transaction_type = build_transaction_type(circumstance_interest_raw)
                
                for transaction_detail in transaction_details:
                    final_record = {
                        'shareholder_name': shareholder_name if shareholder_name else None,
                        'transaction_type': transaction_type if transaction_type else None,
                        **transaction_detail,
                        **individual_share_data
                    }

                    all_records.append(final_record)

            if not is_special_value:
                if all_records and len(all_records) == 1: 
                    record = all_records[0]
                    if record.get('shares_before') == record.get('shares_after'):
                        LOGGER.info(f"[sgx_filings] Skipping {pdf_url}: Single record with no share change")
                        return None 

                if all_records and len(all_records) > 1:
                    seen_share_data = set()
                    for record in all_records:
                        share_data = (
                            record.get('shares_before'),
                            record.get('shares_after')
                        )
                        seen_share_data.add(share_data)
                    
                    if len(seen_share_data) == 1:
                        LOGGER.info(f"[sgx_filings] Skipping {pdf_url}: Multiple shareholders with identical share data")
                        return None

            return all_records

    except requests.RequestException as error:
        LOGGER.error(f"[sgx_filings] Failed to download PDF {pdf_url}: {error}")
        return None
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Error extracting share records from {pdf_url}: {error}", exc_info=True)
        return None
    

def extract_all_fields(doc_fitz: fitz.Document, pdf_url: str) -> list[dict]:
    try:
        # Check if data valid or not 
        type_securities_raw = extract_type_securities_checkbox(
            doc_fitz, r"Type of securities.*?transaction"
        )
        
        if not type_securities_raw.get("results", {}).get('Voting shares/units', False):
            return None  
        
        # Orchestrate record extraction 
        all_records = extract_records(pdf_url, doc_fitz)
    
        # Add fallback for document multiples shareholder
        if all_records:
            apply_fallback_for_multiple_shareholder(all_records, doc_fitz)

        return all_records
        
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to process extract all fields {pdf_url}: {error}", exc_info=True)
        return None 


def get_sgx_filings(url: str) -> list[SGXFilings] | None:
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        payload_html = extract_html_content(soup)

        pdf_url = payload_html.get('url')
        symbol = payload_html.get('symbol')

        LOGGER.info(f"Extracting detail filing for url: {url} pdf_url: {pdf_url}")

        doc_fitz = open_pdf(pdf_url)
        
        if not symbol:
            symbol_extracted = extract_symbol_fallback(doc_fitz)
            symbol = matching_symbol(symbol_extracted)

        list_data_extracted = extract_all_fields(doc_fitz, pdf_url)
        
        if not list_data_extracted:
            return None

        final_filings_list = []
        for data_record in list_data_extracted:
            sgx_filings = SGXFilings(symbol=symbol, url=pdf_url, **data_record)
            final_filings_list.append(sgx_filings)

            print(json.dumps(asdict(sgx_filings), indent=2))
        
        return final_filings_list
    
    except requests.RequestException as error:
        LOGGER.error(f"[sgx filings] Error fetching SGX filing url {url}: {error}", exc_info=True)
        return None

    except Exception as error:
        LOGGER.error(f"[sgx filings] Unexpected Error extracting SGX filings url: {url}: {error}", exc_info=True)
        return None


if __name__ == '__main__':
    test_clean_data = 'https://links.sgx.com/1.0.0/corporate-announcements/L7QIXIV1LZ9CQR8X/d159e63ab68b983fa8f0e286519b84185c47cc3891f0c7a5fb778e529f448a38'
    test_one_name_multiple_shareholder = 'https://links.sgx.com/1.0.0/corporate-announcements/07KOA264E5YBKSP7/59c7b3af10cf9d7ec43bb98a405d2959cce4a0f956347332522f4ab342f96967'
    multiples = 'https://links.sgx.com/1.0.0/corporate-announcements/UQETVC6UVOBCI39D/c7d80525b311a0e0134c87602df7c340edbfd5468b271338eee4cba6812b347f'

    result_sgx_filing = get_sgx_filings(multiples)

    # print(result_sgx_filing)
    # if result_sgx_filing is not None:
    #     for result in result_sgx_filing:
    #         print(json.dumps(asdict(result), indent=2))

    # uv run -m src.fetch_sgx_filings.parser_sgx_filings