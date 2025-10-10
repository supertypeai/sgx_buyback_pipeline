from bs4 import BeautifulSoup 

from src.utils.sgx_parser_helper import safe_convert_float
from src.config.settings import LOGGER


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


def extract_transaction_type(shares_before_percentage: float, shares_after_percentage: float) -> str | None:
    if shares_before_percentage is None or shares_after_percentage is None:
        return None
    
    try:
        if shares_before_percentage < shares_after_percentage:
            transaction_type = 'buy'
        elif shares_after_percentage < shares_before_percentage:
            transaction_type = 'sell'
        else:
            transaction_type = None
        return transaction_type
    
    except Exception as error:
        return LOGGER.error(f"[extract transaction type] Error: {error}")


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
        if 'share' in raw_value.lower().strip():
            return number_of_stock * safe_convert_float(raw_value)
        return safe_convert_float(raw_value)
    
    except Exception as error:
        return LOGGER.error(f"[build value] Error: {error}")