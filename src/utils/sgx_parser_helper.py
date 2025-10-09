from src.config.settings import LOGGER
from src.utils.symbol_matching_helper import match_company_name


def extract_symbol(issuer_security: str):
    try:
        parts = issuer_security.split('-')
        if len(parts) > 1:
            symbol = parts[-1].strip()
            if symbol:  
                return symbol
    except Exception as error:
        LOGGER.error(f"Failed to extract symbol from split: {error}")

    if issuer_security and len(issuer_security) > 5:
        try:
            company_matched = match_company_name(issuer_security)
            if company_matched:
                return company_matched.get('symbol')
        except Exception as error:
            LOGGER.error(f"Fallback matching symbol failed: {error}")

    return None


