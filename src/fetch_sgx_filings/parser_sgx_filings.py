from bs4 import BeautifulSoup 
from dataclasses import asdict
from io import BytesIO

from src.fetch_sgx_filings.utils.payload_helper import (
    extract_section_data, build_transaction_type, 
    build_price_per_share, build_value,
    extract_circumstance_interest_checkbox,
    extract_type_securities_checkbox,
    extract_shares_table
)
from src.utils.sgx_parser_helper import (
    extract_symbol, 
    matching_symbol,
    safe_convert_float, 
    safe_convert_datetime
)
from src.fetch_sgx_filings.models import SGXFilings
from src.config.settings import LOGGER

import fitz
import requests
import re 
import json 


def open_pdf(pdf_url: str):
    if not pdf_url:
        return ''

    try:
        response = requests.get(pdf_url, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        LOGGER.error(f"[sgx_filings] Failed to download PDF: {error}")
        return None
    
    try:
        # Open PDF from memory
        doc = fitz.open(stream=BytesIO(response.content), filetype="pdf")
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to open PDF: {error}")
        return None
    
    return doc 


def parse_pdf(doc):
    full_text = ''
    
    for page_num in range(2, len(doc)):
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
        
        # Add page text to full text
        full_text += "\n".join(sorted_text) + "\n"
    
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
        # Pattern that handles Director/CEO, Substantial Shareholder(s)/Unitholder(s), 
        # and Trustee-Manager/Responsible Person
        pattern = r'acquired or disposed of by (?:Director/CEO|Substantial Shareholders?/Unitholders?|Trustee-Manager/Responsible Person)[^:]*:\s*\n\s*(.+?)(?=\n\s*\d+\.|$)'

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
        # and capture everything after the colon until hit a newline followed by non-value text
        pattern = r'Amount of consideration.*?by (?:Director/CEO|Substantial Shareholders?/Unitholders?|Trustee-Manager/Responsible Person)[^:]*:\s*\n\s*(.+?)(?:\n\n|\n[A-Z]|$)'

        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            raw_value = match.group(1).strip()
            return raw_value
        
        return None
    
    except Exception as error:
        print(f"[sgx_filings] Error extracting total value: {error}")
        return None


def extract_shares_data(pdf_url: str):
    try:
        share_tables = extract_shares_table(pdf_url)
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to extract tables from PDF: {error}")
        return {}, {}

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


def find_all_shareholder_sections(pdf_texts: str) -> list[str]:
    """
    Extract all shareholder sections AND transaction details section.
    Returns: [Shareholder A, Shareholder B, ..., Transaction Details]
    """
    sections = []
    
    # Pattern for shareholder headers
    shareholder_pattern = r'Substantial Shareholder/Unitholder\s+([A-Z])\s*\n\s*Name of Substantial Shareholder'
    
    # Pattern for Part IV
    part_iv_pattern = r'Part IV\s*-\s*Transaction details'
    
    shareholder_matches = list(re.finditer(shareholder_pattern, pdf_texts, re.IGNORECASE))
    part_iv_match = re.search(part_iv_pattern, pdf_texts, re.IGNORECASE)
    
    # If no labeled sections at all, return entire text as single item
    if not shareholder_matches and not part_iv_match:
        return [pdf_texts.strip()]
    
    # Extract shareholder sections
    for i, match in enumerate(shareholder_matches):
        start_pos = match.start()
        
        # Find end position
        if i < len(shareholder_matches) - 1:
            # Ends at next shareholder section
            end_pos = shareholder_matches[i + 1].start()
        elif part_iv_match:
            # Last shareholder ends at Part IV
            end_pos = part_iv_match.start()
        else:
            # No Part IV, goes to end
            end_pos = len(pdf_texts)
        
        sections.append(pdf_texts[start_pos:end_pos].strip())
    
    # Extract Part IV section (everything from Part IV to end)
    if part_iv_match:
        part_iv_section = pdf_texts[part_iv_match.start():].strip()
        sections.append(part_iv_section)
    
    return sections


def extract_all_fields(doc_fitz, pdf_url: str) -> dict[str, any] | None:
    try:
        type_securities_raw = extract_type_securities_checkbox(
            doc_fitz, r"Type of securities.*?transaction"
        )
        type_securities = type_securities_raw.get("results", {})
        print(f'\nraw type securities: {type_securities}\n')

        type_voting_shares = type_securities.get("Voting shares/units", False)

        if not type_voting_shares:
            LOGGER.warning(f'[sgx_filing] Skip processing data for: {pdf_url}')
            return None
        
        # Extract transaction type
        circumstance_interest_raw = extract_circumstance_interest_checkbox(
            doc_fitz, r"Circumstance giving rise to.*?interest"
        )
        print(f'\nraw circumstance: {circumstance_interest_raw}\n')

        transaction_type = build_transaction_type(circumstance_interest_raw)

        # Parse PDF text for other extractions
        pdf_texts = parse_pdf(doc_fitz)

        # Transaction date
        transaction_date = extract_date(pdf_texts)
        transaction_date = safe_convert_datetime(transaction_date)

        # Number of stock
        raw_number_of_stock = extract_number_of_stock(pdf_texts)
        print(f'raw stock: {raw_number_of_stock}')
        number_of_stock = safe_convert_float(raw_number_of_stock)

        # Total value
        raw_value = extract_value(pdf_texts)
        print(f'raw value: {raw_value}')
        value = build_value(raw_value, number_of_stock)

        # Shares before & after
        shares_before_raw, shares_after_raw = extract_shares_data(pdf_url)

        if not shares_before_raw and not shares_after_raw:
            return None

        shares_before = safe_convert_float(shares_before_raw.get("total_shares"))
        shares_before_percentage = safe_convert_float(shares_before_raw.get("percentage"))

        shares_after = safe_convert_float(shares_after_raw.get("total_shares"))
        shares_after_percentage = safe_convert_float(shares_after_raw.get("percentage"))

        # Price per share
        price_per_share = build_price_per_share(raw_value, number_of_stock)
        
        return {
            "transaction_type": transaction_type,
            "transaction_date": transaction_date,
            "number_of_stock": number_of_stock,
            "value": value,
            "price_per_share": price_per_share,
            "shares_before": shares_before,
            "shares_before_percentage": shares_before_percentage,
            "shares_after": shares_after,
            "shares_after_percentage": shares_after_percentage,
        }

    except Exception as error:
        LOGGER.error(f"[extract_buyback_fields] Error parsing SGX filing at {pdf_url}: {error}", exc_info=True)
        return None


def get_sgx_filings(url: str) -> SGXFilings | None:
    try:
        print(f"Extracting detail filing for {url}")

        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        payload_html = extract_html_content(soup)
        pdf_url = payload_html.get('url')
        symbol = payload_html.get('symbol')

        doc_fitz = open_pdf(pdf_url)
        data_extracted = extract_all_fields(doc_fitz=doc_fitz, pdf_url=pdf_url)
        if not data_extracted:
            return None

        sgx_filings = SGXFilings(symbol=symbol, url=pdf_url, **data_extracted)
        # print(json.dumps(asdict(sgx_filings), indent=2))
        return sgx_filings

    except requests.RequestException as error:
        LOGGER.error(f"[sgx filings] Error fetching SGX filing url {url}: {error}", exc_info=True)
        return None

    except Exception as error:
        LOGGER.error(f"[sgx filings] Unexpected Error extracting SGX filings url: {url}: {error}", exc_info=True)
        return None



if __name__ == '__main__':
    test_clean_data = 'https://links.sgx.com/1.0.0/corporate-announcements/L7QIXIV1LZ9CQR8X/d159e63ab68b983fa8f0e286519b84185c47cc3891f0c7a5fb778e529f448a38'
    test_nan_data = 'https://links.sgx.com/1.0.0/corporate-announcements/BMBITXAZI1YQF9G0/2801545bc221a503942782defacc7daba12e6f19d13cfd5fa4aa3a574706481a'
    test_multiple = 'https://links.sgx.com/1.0.0/corporate-announcements/FI2V1DZBQ5O2101M/c37a39864c1f4f847d7a7ce3cb1231babe596d80265cb53e4b5278f099df7852'
    test_value_share = 'https://links.sgx.com/1.0.0/corporate-announcements/I87MUTXD0WEHJ0U1/41091f8b6048d633651f59c319eeb16eeab959c71f25d6f226e9593c1f8ae431'
    test_other_circumstance = 'https://links.sgx.com/1.0.0/corporate-announcements/LOCRN665G3RH5B7X/dde642cff64d558552a329e8662bfb7f014f9d7bfd6492a9b7cc8f1ab3aaa30c'
    test_new_table = 'https://links.sgx.com/1.0.0/corporate-announcements/X8XJZMOQBPABY73A/c7edbbb1ca0cabd332994589a635c66e59700c9b3937b529a49ca4e6de68405e'
    new_test = 'https://links.sgx.com/1.0.0/corporate-announcements/OIN7HMELNIZHXG0B/f69ce5db7f5b2badb47f354ebe015a122c7c211cf293087461ce290bf394f7cb'
    test_failed = 'https://links.sgx.com/1.0.0/corporate-announcements/2QL6D332RYU7A0AM/6c2045604d7c2b64c42d0efb9663703f98dda8b98e846092d32810900ee4a823'
    result_sgx_filing = get_sgx_filings('https://links.sgx.com/1.0.0/corporate-announcements/4L30DFCB3DFLCMNP/1b96579fa316d719251bdcab545fef62e112faf1fd8a5a6a847ab6a279c557f6')

    # print(result_sgx_filing)
    if result_sgx_filing is not None:
        print(json.dumps(asdict(result_sgx_filing), indent=2))