from bs4 import BeautifulSoup 
from datetime import datetime
from dataclasses import asdict

from src.fetch_sgx_buyback.models import SGXAnnouncement
from src.fetch_sgx_buyback.utils.payload_standardize_helper import (
    build_price_per_share, safe_convert_float,
    safe_extract_value, safe_convert_datetime,
    extract_symbol
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
        
        # Get type of buy back 
        on_market = section_a.get('Purchase made by way of market acquisition', None)
        off_market = section_b.get('Purchase made by way of off-market acquisition on equal access scheme', None)
        
        if on_market == 'Yes' and off_market == 'No':
            buy_back_type = 'On Market'
        elif on_market == 'No' and off_market == 'Yes':
            buy_back_type = 'Off Market' 
        
        # Get purchase date 
        purchase_date_raw = section_a.get('Date of Purchase', None)
        purchase_date = safe_extract_value(purchase_date_raw)
        purchase_date = safe_convert_datetime(purchase_date)

        # Get start date 
        start_date_raw = additional_detail.get('Start date for mandate of daily share buy-back')
        start_date = safe_convert_datetime(start_date_raw)

        # Get price per share
        price_paid_per_share = section_a.get('Price Paid per share', None)
        highest_per_share = section_a.get('Highest Price per share', None) 
        lowest_per_share = section_a.get('Lowest Price per share', None)

        price_per_share = build_price_per_share(
            url, price_paid_per_share, highest_per_share, lowest_per_share
        )                           
     
        # Get total number of shares purchased 
        total_share_purchased_raw = section_a.get('Total Number of shares purchased', None) 
        total_share_purchased = safe_extract_value(total_share_purchased_raw)
        total_share_purchased = safe_convert_float(url, total_share_purchased)

        # Get Cumulative No. of shares purchased to date
        cumulative_raw = section_c.get('Total', None)
        cumulative_share_purchased = safe_extract_value(cumulative_raw)
        cumulative_share_purchased = safe_convert_float(url, cumulative_share_purchased)

        # Get total consideration 
        total_consideration_raw = section_a.get('Total Consideration', None) 
        total_consideration = safe_convert_float(url, total_consideration_raw)

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

        return announcement
    
    except requests.RequestException as error:
        LOGGER.error(f"Error fetching SGX announcements for url {url}: {error}", exc_info=True)
        return
    
    except Exception as error:
        LOGGER.error(f"Unexpected Error extracting SGX details: {error}")
        raise


if __name__ == '__main__':
    test_url = 'https://links.sgx.com/1.0.0/corporate-announcements/HZ9DTI92NDMJ7P38/be71e31a63ee4cb10f397f2a6488310c036a2feafda10bb4ea553a1e4728cb14'
    test_url2 = 'https://links.sgx.com/1.0.0/corporate-announcements/JLBE8Z6N8I6ULAUA/9ed94150ff45f589cfb616081586a00d7d4955024b88d74d4798f2c50a673118'
    test_url3 = 'https://links.sgx.com/1.0.0/corporate-announcements/CUF9USLIQ2TIJI41/f09101b4983c39a6444c0c5b050706912108f2c011dcb47eb8b11ee0d5ebe85a'
    test_url4 = 'https://links.sgx.com/1.0.0/corporate-announcements/FJ6J6SPUITM29J2T/47890869fe6d5e2a38506ac65b2ec391d40ff0a2679e692bb8fb5f459cc42d3c'
    result = get_sgx_announcements(test_url4)
    print(json.dumps(asdict(result), indent=2))







