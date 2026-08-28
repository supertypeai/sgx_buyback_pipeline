from pathlib import Path


# SGX FILINGS
SGX_FILINGS_BASE_DIR = Path("data/scraper_output/sgx_filing")
SGX_FILINGS_BASE_DIR.mkdir(parents=True, exist_ok=True)

SGX_FILINGS_PATH_TODAY = SGX_FILINGS_BASE_DIR / "sgx_filings_today.json"
SGX_FILINGS_PATH_YESTERDAY = SGX_FILINGS_BASE_DIR / "sgx_filings_yesterday.json"
SGX_FILINGS_PATH_INSERTABLE = SGX_FILINGS_BASE_DIR / "sgx_filings_insertable.json"
SGX_FILINGS_PATH_NOT_INSERTABLE = SGX_FILINGS_BASE_DIR / "sgx_filings_not_insertable.json"
SGX_FILINGS_PATH_NOT_TOP_70 = SGX_FILINGS_BASE_DIR / "sgx_filings_not_top_70.csv"
SGX_FILINGS_PATH_NOT_TOP_200 = SGX_FILINGS_BASE_DIR / "sgx_filings_not_top_200.csv"
SGX_FILINGS_PATH_TOP_100 = SGX_FILINGS_BASE_DIR / "sgx_filings_top_100.json"

# SGX BUYBACKS
SGX_BUYBACKS_BASE_DIR = Path("data/scraper_output/sgx_buyback")
SGX_BUYBACKS_BASE_DIR.mkdir(parents=True, exist_ok=True)

SGX_BUYBACKS_PATH_TODAY = SGX_BUYBACKS_BASE_DIR / "sgx_buybacks_today.json"
SGX_BUYBACKS_PATH_YESTERDAY = SGX_BUYBACKS_BASE_DIR / "sgx_buybacks_yesterday.json"
SGX_BUYBACKS_PATH_NOT_TOP_70 = SGX_BUYBACKS_BASE_DIR / "sgx_buybacks_not_top_70.csv"
SGX_BUYBACKS_PATH_NOT_TOP_200 = SGX_BUYBACKS_BASE_DIR / "sgx_buybacks_not_top_200.csv"

# UPCOMING DIVIDEND
SGX_UPCOMING_DIVIDEND_BASE_DIR = Path("data/scraper_output/sgx_upcoming_dividend")
SGX_UPCOMING_DIVIDEND_BASE_DIR.mkdir(parents=True, exist_ok=True)

UPCOMING_DIVIDEND_NOT_TOP_200 = SGX_UPCOMING_DIVIDEND_BASE_DIR / "upcoming_dividend_not_top_200.csv"  
UPCOMING_DIVIDEND_TOP_200 = SGX_UPCOMING_DIVIDEND_BASE_DIR / "upcoming_dividend_top_200.json"
UPCOMING_DIVIDEND = SGX_UPCOMING_DIVIDEND_BASE_DIR / "upcoming_dividend.json"

OUTPUT_DIR_SHAREHOLDERS = Path('data/scraper_output/shareholders')

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "x-test": "true",
}

MODEL_CONFIG = { 
    "nvidia-nemotron-3-ultra": {
        "model": "nvidia/nemotron-3-ultra-550b-a55b:free", 
        "provider": "openrouter"
    },
    'gpt-oss-120b': {
        'model': 'openai/gpt-oss-120b',
        'provider': 'groq', 
        # 'key': GROQ_API_KEY
    },
    'gpt-oss-20b': {
        'model': 'openai/gpt-oss-20b',
        'provider': 'groq',
        # 'key': GROQ_API_KEY
    },
    'deepseek-v4-flash': {
        'model': 'deepseek/deepseek-v4-flash-0731',
        'provider': 'openrouter'
    },
    'laguna-s-2.1': {
        'model': 'poolside/laguna-s-2.1:free',
        'provider': 'openrouter'
    }
}

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

ROTATE_STATUS_CODES = {401, 403, 429, 413}

# A pool of one key is exhausted by a single burst 429, so the pool is swept again.
ROTATE_MAX_SWEEPS = 3
ROTATE_BACKOFF_SECONDS = 20

LLM_TIMEOUT_SECONDS = 60
ABORT_STATUS_CODES = {400, 422, 500, 502, 503, 504}

ROTATE_KEYWORDS = (
    "rate limit", "too many requests", "authentication", "invalid api key", 
    "request too large"
)
ROTATE_400_KEYWORDS = ("organization_restricted",)
ABORT_KEYWORDS = (
    "context length", "max token", "internal server",
    "bad gateway", "service unavailable",
)

SGX_SYMBOL_SUFFIX = ".SI"