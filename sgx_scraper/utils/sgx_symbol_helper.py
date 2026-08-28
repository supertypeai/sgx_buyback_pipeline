from sgx_scraper.utils.constant import SGX_SYMBOL_SUFFIX


def add_sgx_suffix(symbol: str | None) -> str | None:
    """
    Return the symbol with the SGX Yahoo-style suffix (e.g. 'D05' -> 'D05.SI').
    """
    if not symbol:
        return symbol

    symbol = symbol.strip()

    if symbol.upper().endswith(SGX_SYMBOL_SUFFIX):
        return f"{symbol[: -len(SGX_SYMBOL_SUFFIX)]}{SGX_SYMBOL_SUFFIX}"

    return f"{symbol}{SGX_SYMBOL_SUFFIX}"


def strip_sgx_suffix(symbol: str | None) -> str | None:
    """
    Return the bare symbol without the SGX suffix (e.g. 'D05.SI' -> 'D05').
    """
    if not symbol:
        return symbol

    symbol = symbol.strip()

    if symbol.upper().endswith(SGX_SYMBOL_SUFFIX):
        return symbol[: -len(SGX_SYMBOL_SUFFIX)]

    return symbol
