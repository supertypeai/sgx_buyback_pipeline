from email.mime.application import MIMEApplication

from sgx_scraper.config.settings import LOGGER

import html 
import yfinance as yf 
import pandas as pd 


def escape_keyword(value):
    return html.escape(str(value)) if value is not None else "-"


def attach_files(file_path: str, msg):
    try:
        with open(file_path, "rb") as f:
            part = MIMEApplication(f.read())
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{file_path.split("/")[-1]}"'
            )
            msg.attach(part)

    except Exception as error:
        LOGGER.error(f"[attach_files] Could not attach file {file_path}: {error}")


def get_price(symbol: str, date_str: str) -> float:
    try:
        ticker_symbol = f'{symbol}.SI'
        ticker = yf.Ticker(ticker_symbol)

        data = ticker.history(start=date_str, end=pd.to_datetime(date_str) + pd.Timedelta(days=3))
        
        if data.empty:
            return None
        
        return float(data["Close"].iloc[0])
    
    except Exception as error:
        LOGGER.error(f'[get_price] Error: {error}')