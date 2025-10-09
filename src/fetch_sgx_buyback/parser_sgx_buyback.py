from bs4 import BeautifulSoup 
from dataclasses import asdict

from src.fetch_sgx_buyback.models import SGXAnnouncement
from src.fetch_sgx_buyback.utils.payload_standardize_helper import (
    build_price_per_share, safe_convert_float,
    safe_extract_value, safe_convert_datetime,
    extract_symbol, safe_extract_fallback
)
from src.config.settings import LOGGER

import requests
import json 


def extract_table_data(table_element) -> dict[str, str | list[str]]:
    table_data = {}
    rows = table_element.find_all('tr')
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) < 2:
            continue
            
        key = cells[0].get_text(strip=True)
        values = []
        for cell in cells[1:]:
            text = cell.get_text(strip=True)
            if text:
                values.append(text)
        
        if not key or not values:
            continue
            
        if len(values) == 1:
            table_data[key] = values[0]
        else:
            table_data[key] = values
            
    return table_data


def extract_section_data(soup: BeautifulSoup, section_title: str) -> dict[str, str | list[str]]:
    section_data = {}
    h2 = soup.find('h2', class_='announcement-group-header', string=section_title)
    if not h2: 
        return section_data
    
    section_div = h2.find_next_sibling('div', class_='announcement-group')
    if not section_div:
        return section_data
    
    # Extract simple key-value pairs where the <dd> does not contain a table
    dt_tags = section_div.find_all('dt')
    for dt in dt_tags:
        dd = dt.find_next_sibling('dd')
        if dd and not dd.find('table'):
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            if key:
                section_data[key] = value

    # Find all tables within the section 
    all_tables = section_div.find_all('table')
    for table in all_tables:
        table_data = extract_table_data(table)
        section_data.update(table_data)

    for key in list(section_data.keys()):
        if 'total consideration' in key.lower().strip():
            section_data['Total Consideration'] = section_data.pop(key)
    
    return section_data


def get_sgx_announcements(url: str) -> SGXAnnouncement: 
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        print(f"Extracting detail for {url}")

        # Extract per sections
        issuer_section = extract_section_data(soup, "Issuer & Securities")
        additional_detail = extract_section_data(soup, 'Additional Details')

        section_a = extract_section_data(soup, 'Section A')
        section_b = extract_section_data(soup, 'Section B')
        section_c = extract_section_data(soup, 'Section C')
        section_d = extract_section_data(soup, 'Section D')
        
        # Get symbol
        issuer_security = issuer_section.get('Securities', None)
        symbol = extract_symbol(issuer_security)
        if not symbol:
            issuer_name = issuer_section.get('Issuer/ Manager')
            symbol = extract_symbol(issuer_name)
        
        # Get type of buy back 
        on_market = section_a.get('Purchase made by way of market acquisition', None)
        off_market = section_b.get('Purchase made by way of off-market acquisition on equal access scheme', None)
        
        if on_market == 'Yes' and off_market == 'No':
            buy_back_type = 'On Market'
        elif on_market == 'No' and off_market == 'Yes':
            buy_back_type = 'Off Market' 
        
        # Get purchase date 
        purchase_date = safe_extract_fallback('Date of Purchase', section_a, section_b)
        purchase_date = safe_convert_datetime(purchase_date)

        # Get start date 
        start_date_raw = additional_detail.get('Start date for mandate of daily share buy-back')
        start_date = safe_convert_datetime(start_date_raw)

        # Get price per share
        price_paid_per_share = safe_extract_fallback('Price Paid per share', section_a, section_b)
        if not price_paid_per_share:
            price_paid_per_share = safe_extract_fallback('Price Paid or Payable per Share', section_a, section_b)
        highest_per_share = safe_extract_fallback('Highest Price per share', section_a, section_b) 
        lowest_per_share = safe_extract_fallback('Lowest Price per share', section_a, section_b)

        price_per_share = build_price_per_share(
            url, price_paid_per_share, highest_per_share, lowest_per_share
        )                           
     
        # Get total number of shares purchased 
        total_share_purchased= safe_extract_fallback('Total Number of shares purchased', section_a, section_b) 
        total_share_purchased = safe_convert_float(url, total_share_purchased)

        # Get Cumulative No. of shares purchased to date
        cumulative_raw = section_c.get('Total', None)
        cumulative_share_purchased = safe_extract_value(cumulative_raw)
        cumulative_share_purchased = safe_convert_float(url, cumulative_share_purchased)

        # Get total consideration 
        total_consideration = safe_extract_fallback('Total Consideration', section_a, section_b) 
        total_consideration = safe_convert_float(url, total_consideration)

        # Get Number of treasury shares held after purchase
        treasury_shares_after_purchase_raw = section_d.get('Number of treasury shares held after purchase', None)
        treasury_shares_after_purchase = safe_convert_float(url, treasury_shares_after_purchase_raw)

        announcement = SGXAnnouncement(
            url=url,
            symbol=symbol,
            purchase_date=purchase_date,
            type=buy_back_type,
            start_date=start_date,
            price_per_share=price_per_share,
            total_value=total_consideration,
            total_shares_purchased=total_share_purchased,
            cumulative_purchased=cumulative_share_purchased,
            treasury_shares_after_purchase=treasury_shares_after_purchase,
        )
        print(json.dumps(asdict(announcement), indent=2))
        return announcement
    
    except requests.RequestException as error:
        LOGGER.error(f"Error fetching SGX announcements for url {url}: {error}", exc_info=True)
        return
    
    except Exception as error:
        LOGGER.error(f"Unexpected Error extracting SGX details: {error}", exc_info=True)
        raise


if __name__ == '__main__':
    test_url = 'https://links.sgx.com/1.0.0/corporate-announcements/TWVRNZJLN1NIW9AC/381e0dd5a56206e480e57e449a11481600a986867bd4e8b4b57151013bfc0602'
    result = get_sgx_announcements(test_url)
    print(json.dumps(asdict(result), indent=2))







