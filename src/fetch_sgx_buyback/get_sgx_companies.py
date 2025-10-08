from datetime import datetime, timedelta

from src.config.settings import SUPABASE_CLIENT

import json 
import os 

CACHE_PATH = 'data/sgx_companies.json'

def get_sgx_companies():
    try:
        response = (
            SUPABASE_CLIENT
            .table('sgx_companies')
            .select('name, symbol')
            .execute()
        )
        return response.data

    except Exception as error:
        print(f"Error fetching SGX companies: {error}")
        return None


def refresh_master_company_data(force_refresh: bool = True): 
    if os.path.exists(CACHE_PATH) and not force_refresh:
        modified_time = datetime.fromtimestamp(os.path.getmtime(CACHE_PATH))
        if datetime.now() - modified_time < timedelta(days=1):
            print("Using cached sgx_companies.json")
            return
        
    data = get_sgx_companies()

    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        
    print(f"Saved {len(data)} companies to company_data.json")


if __name__ == '__main__':
    refresh_master_company_data()