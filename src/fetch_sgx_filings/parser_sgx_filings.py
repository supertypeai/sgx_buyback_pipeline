from bs4 import BeautifulSoup 
from dataclasses import asdict
from io import BytesIO

from src.fetch_sgx_filings.utils.payload_helper import (
    extract_section_data, extract_transaction_type,
    build_price_per_share, build_value
)
from src.fetch_sgx_filings.models import SGXFilings
from src.config.settings import LOGGER
from src.utils.sgx_parser_helper import (
    extract_symbol, 
    matching_symbol,
    safe_convert_float, 
    safe_convert_datetime
)

import fitz
import requests
import re 
import json 


def parse_pdf(pdf_path: str) -> str:
    if not pdf_path:
        return ''

    try:
        response = requests.get(pdf_path, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        LOGGER.error(f"[sgx_filings] Failed to download PDF: {error}")
        return ""

    try:
        # Open PDF from memory
        doc = fitz.open(stream=BytesIO(response.content), filetype="pdf")
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed to open PDF: {error}")
        return ""

    full_texts = ""
    try:
        # Loop through pages starting from page 3
        for page_num in range(2, len(doc)):
            try:
                page = doc.load_page(page_num)
                text = page.get_text("text")
                full_texts += text
            except Exception as error:
                LOGGER.warning(f"[sgx_filings] Failed to read page {page_num}: {error}")
                continue
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Failed during PDF text extraction: {error}")
        return ""
    
    finally:
        doc.close()

    return full_texts.strip()


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


def extract_date(url: str, text: str) -> str | None:
    try:
        match = re.search(
            r'Date of acquisition of or change in interest:.*?(\d{2}-[A-Za-z]{3}-\d{4})',
            text,
            re.DOTALL
        )
        if match:
            return match.group(1)
        else:
            LOGGER.warning(f"[sgx_filings] Date of acquisition not found, url: {url}")
            return None
    except Exception as error:
        print(f"[sgx_filings] Error extracting date: {error} url: {url}")
        return None


def extract_number_of_stock(pdf_url: str, text: str) -> str | None:
    try:
        patterns = [
            r'acquired or disposed of by Director/CEO:\s*\n\s*([\d,]+|N\.A)',
            r'acquired or disposed of by Substantial Shareholders/Unitholders:\s*\n\s*([\d,]+|N\.A)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                value = match.group(1).strip()
                if value.upper() not in ['N.A', 'N.A.']:
                    try:
                        return value
                    except ValueError:
                        continue
        return None
    
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Error extracting number of stock: {error}, url: {pdf_url}")
        return None


def extract_value(pdf_url: str, text: str) -> str | None:
    try:
        patterns = [
            # Match "Amount of consideration ... by Director/CEO" followed by an amount like "S$123,456", "S$123,456/share", or "N.A"
            r'Amount of consideration.*?by Director/CEO.*?:\s*\n\s*(S\$[\d,]+\.?\d*(?:/share)?|N\.A)',

            # Match "Amount of consideration ... by Substantial Shareholders/Unitholders" followed by an amount like "S$123,456", "S$123,456/share", or "N.A"
            r'Amount of consideration.*?by Substantial Shareholders/Unitholders.*?:\s*\n\s*(S\$[\d,]+\.?\d*(?:/share)?|N\.A)',

            # Match "Amount of consideration ... by Director/CEO" followed by a decimal per-share value like "S$1.23/share" or "N.A"
            r'Amount of consideration.*?by Director/CEO.*?:\s*\n\s*(S\$\d+\.\d+/share|N\.A)',

            # Match "Amount of consideration ... by Substantial Shareholders/Unitholders" followed by a decimal per-share value like "S$1.23/share" or "N.A"
            r'Amount of consideration.*?by Substantial Shareholders/Unitholders.*?:\s*\n\s*(S\$\d+\.\d+/share|N\.A)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                value = match.group(1).strip()
                if value.upper() not in ['N.A', 'N.A.'] and value.startswith('S$'):
                    return value
        return None
    
    except Exception as error:
        LOGGER.error(f"[sgx_filings] Error extracting total value: {error}, url: {pdf_url}")
        return None


def extract_shares_data(pdf_url: str, text: str) -> tuple[dict, dict]:
    if 'ordinary voting shares/units' not in text.lower():
        return {}, {}
    
    try:
        number_pattern = r'[\d,]+'
        percentage_pattern = r'[\d.]+'
        
        before_pattern = (
            rf'Immediately before the transaction.*?'
            rf'No\. of ordinary voting shares/units held:\s*'
            rf'{number_pattern}\s+'  # Skip Direct Interest
            rf'{number_pattern}\s+'  # Skip Deemed Interest
            rf'({number_pattern})\s+'  # Capture Total
            rf'As a percentage.*?'
            rf'{percentage_pattern}\s+'  # Skip Direct Interest %
            rf'{percentage_pattern}\s+'  # Skip Deemed Interest %
            rf'({percentage_pattern})'  # Capture Total %
        )

        after_pattern = (
            rf'Immediately after the transaction.*?'
            rf'No\. of ordinary voting shares/units held:\s*'
            rf'{number_pattern}\s+'  # Skip Direct Interest
            rf'{number_pattern}\s+'  # Skip Deemed Interest
            rf'({number_pattern})\s+'  # Capture Total
            rf'.*?As a percentage.*?'
            rf'{percentage_pattern}\s+'  # Skip Direct Interest %
            rf'{percentage_pattern}\s+'  # Skip Deemed Interest %
            rf'({percentage_pattern})'  # Capture Total %
        )
        
        before_match = re.search(before_pattern, text, re.DOTALL)
        after_match = re.search(after_pattern, text, re.DOTALL)

        shares_before = {}
        shares_after = {}
        
        if before_match:
            shares_before = {
                'total_shares': before_match.group(1),
                'percentage': before_match.group(2)
            }
        
        if after_match:
            shares_after = {
                'total_shares': after_match.group(1),
                'percentage': after_match.group(2)
            }
        
        return shares_before, shares_after
    
    except Exception as error:
        LOGGER.error(f'[sgx_filings] Error extracting share before after: {error} url: {pdf_url}')
        return {}, {}


def get_sgx_filings(url: str) -> SGXFilings:
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        payload_html = extract_html_content(soup)
        pdf_url = payload_html.get('url', None)
        symbol = payload_html.get('symbol', None)

        # Parse pdf to texts
        pdf_texts = parse_pdf(pdf_url)
        
        # Extract transaction date
        transaction_date = extract_date(pdf_url, pdf_texts)
        transaction_date = safe_convert_datetime(transaction_date)

        # Extract number of stock 
        raw_number_of_stock = extract_number_of_stock(pdf_url, pdf_texts)
        number_of_stock = safe_convert_float(raw_number_of_stock)

        # Extract total value
        raw_value = extract_value(pdf_url, pdf_texts)
        value = build_value(raw_value, number_of_stock)

        # Extract shares before and after
        shares_before_raw, shares_after_raw = extract_shares_data(pdf_url, pdf_texts)
        
        shares_before = shares_before_raw.get('total_shares', None)
        shares_before = safe_convert_float(shares_before)

        shares_before_percentage = shares_before_raw.get('percentage', None)
        shares_before_percentage = safe_convert_float(shares_before_percentage)

        shares_after = shares_after_raw.get('total_shares', None)
        shares_after = safe_convert_float(shares_after)
        
        shares_after_percentage = shares_after_raw.get('percentage', None)
        shares_after_percentage = safe_convert_float(shares_after_percentage)
    
        # Extract transaction type 
        transaction_type = extract_transaction_type(shares_before_percentage, shares_after_percentage)

        # Calculate price per share 
        price_per_share = build_price_per_share(raw_value, number_of_stock)

        sgx_filings = SGXFilings(
            url=pdf_url,
            symbol=symbol,
            transaction_date=transaction_date,
            number_of_stock=number_of_stock,
            value=value,
            price_per_share=price_per_share,
            transaction_type=transaction_type,
            shares_before=shares_before,
            shares_before_percentage=shares_before_percentage,
            shares_after=shares_after,
            shares_after_percentage=shares_after_percentage
        )

        print(json.dumps(asdict(sgx_filings), indent=2))
        return sgx_filings
    
    except requests.RequestException as error:
        LOGGER.error(f"[sgx filings] Error fetching SGX filing url {url}: {error}", exc_info=True)
        return
    
    except Exception as error:
        LOGGER.error(f"[sgx filings] Unexpected Error extracting SGX filings url: {url}: {error}", exc_info=True)
        raise


if __name__ == '__main__':
    test_clen_data = 'https://links.sgx.com/1.0.0/corporate-announcements/L7QIXIV1LZ9CQR8X/d159e63ab68b983fa8f0e286519b84185c47cc3891f0c7a5fb778e529f448a38'
    test_nan_data = 'https://links.sgx.com/1.0.0/corporate-announcements/BMBITXAZI1YQF9G0/2801545bc221a503942782defacc7daba12e6f19d13cfd5fa4aa3a574706481a'
    test_multiple = 'https://links.sgx.com/1.0.0/corporate-announcements/FI2V1DZBQ5O2101M/c37a39864c1f4f847d7a7ce3cb1231babe596d80265cb53e4b5278f099df7852'
    test_value_share = 'https://links.sgx.com/1.0.0/corporate-announcements/I87MUTXD0WEHJ0U1/41091f8b6048d633651f59c319eeb16eeab959c71f25d6f226e9593c1f8ae431'
    result_sgx_filing = get_sgx_filings(test_value_share)
    print(json.dumps(asdict(result_sgx_filing), indent=2))