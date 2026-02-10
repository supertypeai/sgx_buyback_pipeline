from sgx_scraper.config.settings import SUPABASE_CLIENT

import json 
import os 

SGX_LOOKUP_PATH = 'data/sgx_companies.json'
SGX_PATH = 'data/sgx_companies.json'

def get_sgx_companies():
    try:
        response = (
            SUPABASE_CLIENT
            .table('sgx_companies')
            .select('name, symbol', 'sector', 'sub_sector')
            .execute()
        )
        return response.data

    except Exception as error:
        print(f"Error fetching SGX companies: {error}")
        return None


def refresh_master_company_data_lookup():     
    datas = get_sgx_companies()

    sgx_lookup = {}
    for data in datas: 
        symbol = data.get('symbol') 
        sgx_lookup[symbol] = data

    sgx_lookup_list = [sgx_lookup]

    os.makedirs(os.path.dirname(SGX_LOOKUP_PATH), exist_ok=True)
    with open(SGX_LOOKUP_PATH, 'w', encoding='utf-8') as file:
        json.dump(sgx_lookup_list, file, ensure_ascii=False, indent=2)
        
    print(f"Saved {len(sgx_lookup)} companies to data/sgx_companies_lookup.json")


def refresh_master_company_data():     
    data = get_sgx_companies()

    os.makedirs(os.path.dirname(SGX_PATH), exist_ok=True)
    with open(SGX_PATH, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        
    print(f"Saved {len(data)} companies to data/sgx_companies.json")


if __name__ == '__main__':
    refresh_master_company_data()
    refresh_master_company_data_lookup()