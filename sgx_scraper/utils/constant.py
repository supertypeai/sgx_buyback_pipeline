from pathlib import Path


# SGX FILINGS
SGX_FILINGS_BASE_DIR = Path("data/scraper_output/sgx_filing")
SGX_FILINGS_BASE_DIR.mkdir(parents=True, exist_ok=True)

SGX_FILINGS_PATH_TODAY = SGX_FILINGS_BASE_DIR / "sgx_filings_today.json"
SGX_FILINGS_PATH_YESTERDAY = SGX_FILINGS_BASE_DIR / "sgx_filings_yesterday.json"
SGX_FILINGS_PATH_INSERTABLE = SGX_FILINGS_BASE_DIR / "sgx_filings_insertable.json"
SGX_FILINGS_PATH_NOT_INSERTABLE = SGX_FILINGS_BASE_DIR / "sgx_filings_not_insertable.json"
SGX_FILINGS_PATH_NOT_TOP_70 = SGX_FILINGS_BASE_DIR / "sgx_filings_not_top_70.csv"


# SGX BUYBACKS
SGX_BUYBACKS_BASE_DIR = Path("data/scraper_output/sgx_buyback")
SGX_BUYBACKS_BASE_DIR.mkdir(parents=True, exist_ok=True)

SGX_BUYBACKS_PATH_TODAY = SGX_BUYBACKS_BASE_DIR / "sgx_buybacks_today.json"
SGX_BUYBACKS_PATH_YESTERDAY = SGX_BUYBACKS_BASE_DIR / "sgx_buybacks_yesterday.json"
SGX_BUYBACKS_PATH_NOT_TOP_70 = SGX_BUYBACKS_BASE_DIR / "sgx_buybacks_not_top_70.csv"