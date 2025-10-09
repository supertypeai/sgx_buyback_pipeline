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


def refresh_master_company_data():     
    data = get_sgx_companies()

    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        
    print(f"Saved {len(data)} companies to data/sgx_companies.json")


if __name__ == '__main__':
    refresh_master_company_data()