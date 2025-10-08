from datetime import datetime 

from src.config.settings import SUPABASE_CLIENT

import typer


def normalize_datetime(date: str) -> str: 
    try:
        if '-' in date: 
            dt = datetime.strptime(date, "%Y-%m-%d")
        else:
            dt = datetime.strptime(date, "%Y%m%d")
        
        return dt.strftime("%Y%m%d") 
    
    except ValueError:
        raise typer.BadParameter(
            "Invalid date format. Use YYYY-MM-DD or YYYYMMDD."
        )
    

def push_to_db(sgx_buback_payload):
    pass 