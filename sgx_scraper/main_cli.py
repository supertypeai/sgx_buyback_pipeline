from sgx_scraper.fetch_sgx_buyback.cli import app as buyback_app
from sgx_scraper.fetch_sgx_filings.cli import app as filings_app
from sgx_scraper.fetch_shareholders.cli import app as shareholders_app
from sgx_scraper.fetch_upcoming_dividend.cli import app as dividend_app
from sgx_scraper.fetch_reit_transaction.cli import app as reit_transaction_app
from sgx_scraper.fetch_agm.cli import app as agm_app
from sgx_scraper.fetch_managements.cli import app as management_app
from sgx_scraper.fetch_takeover_offers.cli import app as takeover_app

import typer
import logging
import sys


def setup_logging():
    """Configures logging for the whole application"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("scraper.log") 
        ]
    )

    # suppress noisy third-party loggers
    logging.getLogger('WDM').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire2').setLevel(logging.WARNING)
    logging.getLogger('mitmproxy').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)


app = typer.Typer(
    help='A CLI for managing scraper sgx buybacks and filings',
    no_args_is_help=True
)


@app.callback()
def main():
    """
    SGX Scraper CLI.

    This callback function treats this as a multi-command app
    """
    setup_logging()


# pipeline registration: one line per pipeline
app.add_typer(buyback_app)           # scraper_buybacks
app.add_typer(filings_app)           # scraper_filings
app.add_typer(shareholders_app)      # track_shareholders, sync_screener_shareholders
app.add_typer(dividend_app)          # upcoming_dividend
app.add_typer(reit_transaction_app)  # scraper_reit_transaction
app.add_typer(agm_app)               # scraper_agm
app.add_typer(management_app)        # track_management, scraper_managements
app.add_typer(takeover_app)          # takeover


if __name__ == '__main__':
    app()
