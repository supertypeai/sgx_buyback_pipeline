from pathlib import Path

from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.utils.cli_helper import get_db
from sgx_scraper.utils.json_helper import write_json
from sgx_scraper.utils.sgx_symbol_helper import strip_sgx_suffix

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
        response = get_db(
            table="sgx_companies",
            columns=(
                "name,symbol,sector,"
                "sub_sector,shareholders,management,former_management"
            ),
            query=lambda query: (
                query
                .eq('is_suspended', False)
                .eq('is_active', True)
            )
        )   

        for record in response:
            record["symbol"] = strip_sgx_suffix(record["symbol"])

        return response

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

    write_json(
        sgx_path, 
        sgx_lookup
    )    

    print(f"Saved {len(sgx_lookup)} companies to data/sgx_companies.json")


if __name__ == '__main__':
    refresh_master_company_data()
   
