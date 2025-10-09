from datetime import datetime 

from src.config.settings import SUPABASE_CLIENT
from src.config.settings import LOGGER


def normalize_datetime(date: str | datetime) -> str: 
    if isinstance(date, datetime):
        return date.strftime("%Y%m%d")
     
    try:
        if '-' in date: 
            dt = datetime.strptime(date, "%Y-%m-%d")
        else:
            dt = datetime.strptime(date, "%Y%m%d")
        
        return dt.strftime("%Y%m%d") 
    
    except ValueError:
        LOGGER.error("Invalid date format. Use YYYY-MM-DD or YYYYMMDD.")
    

def push_to_db(sgx_buyback_payload: list[dict[str]]):
    if not sgx_buyback_payload:
        LOGGER.info(f'[SGX_BUYBACK] is empty, skipping push to DB')
        return 
    
    try:
        response = (
            SUPABASE_CLIENT
            .table('sgx_buybacks')
            .insert(sgx_buyback_payload)
            .execute()
        )
        LOGGER.info(f"[SGX_BUYBACK] Successfully pushed {len(sgx_buyback_payload)} records to DB")
        return response
    
    except Exception as error:
        LOGGER.error(f"[SGX_BUYBACK] Failed to push data: {error}")
        return None