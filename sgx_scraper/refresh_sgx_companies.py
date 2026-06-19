from pathlib import Path

from sgx_scraper.config.settings import SUPABASE_CLIENT

import json 
import re 


def convert_to_kebab(sub_sector: str):
    result = (
        sub_sector
        .replace("&", "")
        .replace(",", "")
        .replace("  ", " ")
        .replace(" ", "-")
        .lower()
    )
    return re.sub(r'-+', '-', result)


def get_sgx_companies():
    try:
        response = (
            SUPABASE_CLIENT
            .table('sgx_companies')
            .select(
                'name,' 
                'symbol', 
                'sector', 
                'sub_sector', 
                'investing_symbol,' 
                'shareholders,' 
                'management'
            )
            .eq('is_suspended', False)
            .eq('is_active', True)
            .execute()
        )
        return response.data

    except Exception as error:
        print(f"Error fetching SGX companies: {error}")
        return None


def refresh_master_company_data():     
    datas = get_sgx_companies()

    sgx_lookup = {}
    
    for data in datas: 
        symbol = data.get('symbol') 
        data['sector'] = convert_to_kebab(data['sector'])
        data['sub_sector'] = convert_to_kebab(data['sub_sector'])

        sgx_lookup[symbol] = data

    sgx_path = Path('data/sgx_companies.json')

    with sgx_path.open('w', encoding='utf-8') as file:
        json.dump(sgx_lookup, file, ensure_ascii=False, indent=2)
        
    print(f"Saved {len(sgx_lookup)} companies to data/sgx_companies.json")


if __name__ == '__main__':
    refresh_master_company_data()
   