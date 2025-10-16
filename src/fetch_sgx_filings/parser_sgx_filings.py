from bs4 import BeautifulSoup 
from dataclasses import asdict
from io import BytesIO

from src.fetch_sgx_filings.utils.payload_helper import (
    build_transaction_type, 
    build_price_per_share, build_value,
)
from src.utils.sgx_parser_helper import (
    extract_symbol, 
    matching_symbol,
    safe_convert_float, 
    safe_convert_datetime
)
from src.fetch_sgx_filings.utils.payload_pdf_helper import (
    extract_circumstance_interest_checkbox,
    extract_type_securities_checkbox,
    extract_share_tables,
    find_shareholder_sections
)
from src.fetch_sgx_filings.utils.payload_html_helper import extract_section_data
from src.fetch_sgx_filings.models import SGXFilings
from src.config.settings import LOGGER

import fitz
import requests
import re 
import json 
import pdfplumber 
import io 


def open_pdf(pdf_url: str):
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


def parse_pdf(doc: fitz.Document) -> str:
    full_text = ''
    
    for page_num in range(2, len(doc)):
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
            r'Date of acquisition of or change in interest:.*?(\d{2}-[A-Za-z]{3}-\d{4})',
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
        pattern = r'acquired or\s+(?:\d+\.\s+)?disposed of by (?:Director/CEO|Substantial Shareholders?/Unitholders?|Trustee-Manager/Responsible Person)\s*:\s*(.+?)(?=\n\s*\d+\.|$)'

        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
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
        pattern = r'Amount of consideration.*?by (?:Director/CEO|Substantial Shareholders?/Unitholders?|Trustee-Manager/Responsible Person)[^:]*:\s*\n\s*(.+?)(?:\n\n|\n[A-Z]|$)'

        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            raw_value = match.group(1).strip()
            return raw_value
        
        return None
    
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Error extracting total value: {error}")
        return None


def extract_shares_data(pdf_object: pdfplumber.PDF, page_number: int, bbox: tuple) -> tuple[dict, dict]:
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


def get_individual_share_tables(pdf_object: pdfplumber.PDF, page_number: int, bbox: tuple):
    shares_before_raw, shares_after_raw = extract_shares_data(pdf_object, page_number, bbox)

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


def extract_static_fields(doc_fitz: fitz.Document) -> dict[str, any] | None:
    try:
        # Extract transaction type
        circumstance_interest_raw = extract_circumstance_interest_checkbox(
            doc_fitz, r"Circumstance giving rise to.*?interest"
        )
        # print(f'\nraw circumstance: {circumstance_interest_raw}\n')
        transaction_type = build_transaction_type(circumstance_interest_raw)

        # Parse pdf to texts
        pdf_texts = parse_pdf(doc_fitz)

        # Transaction date
        transaction_date = extract_date(pdf_texts)
        transaction_date = safe_convert_datetime(transaction_date)

        # Number of stock
        raw_number_of_stock = extract_number_of_stock(pdf_texts)
        print(f'raw stock: {raw_number_of_stock}')
        number_of_stock = safe_convert_float(raw_number_of_stock)
        
        # Value 
        raw_value = extract_value(pdf_texts)
        print(f'raw value: {raw_value}')
        value = build_value(raw_value, number_of_stock)
        
        # Price per share 
        price_per_share = build_price_per_share(raw_value, number_of_stock)

        return {
            "transaction_type": transaction_type,
            "transaction_date": transaction_date,
            "number_of_stock": number_of_stock,
            "value": value,
            "price_per_share": price_per_share,
        }
    
    except Exception as error:
        LOGGER.error(f'[sgx_filings] Error while extracting static fields: {error}') 
        return None 
    

def extract_all_fields(doc_fitz: fitz.Document, pdf_url: str) -> list[dict]:
    try:
        # Check if data valid or not 
        type_securities_raw = extract_type_securities_checkbox(
            doc_fitz, r"Type of securities.*?transaction"
        )
        
        if not type_securities_raw.get("results", {}).get('Voting shares/units', False):
            return None  

        # Extract static data 
        shared_details = extract_static_fields(doc_fitz)

        # Extract shares table and handle if any multiple shareholders
        response = requests.get(pdf_url)
        response.raise_for_status()
        pdf_file = io.BytesIO(response.content)

        with pdfplumber.open(pdf_file) as pdf:
            
            shareholder_sections = find_shareholder_sections(pdf_url)

            all_records = []
            for shareholder_section in shareholder_sections:
                individual_share_data = get_individual_share_tables(
                    pdf, 
                    shareholder_section['page_number'],
                    shareholder_section['bbox']
                )

                if not individual_share_data:
                    continue
                
                final_record = {
                    **shared_details, 
                    **individual_share_data
                }
                all_records.append(final_record)

            return all_records
        
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to process extract all fields: {error}")
        return None 


def get_sgx_filings(url: str) -> list[SGXFilings] | None:
    try:
        print(f"Extracting detail filing for {url}")
        
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        payload_html = extract_html_content(soup)
        pdf_url = payload_html.get('url')
        symbol = payload_html.get('symbol')

        doc_fitz = open_pdf(pdf_url)
        
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
    double_stock = 'https://links.sgx.com/1.0.0/corporate-announcements/AFQ3ORDPIU508IKT/06f0742724909806730760c77a393f173158cd32fad1f28757d3fbedb7b7fd73'
    result_sgx_filing = get_sgx_filings(double_stock)

    # print(result_sgx_filing)
    # if result_sgx_filing is not None:
    #     for result in result_sgx_filing:
    #         print(json.dumps(asdict(result), indent=2))