from collections import defaultdict

from sgx_scraper.utils.cli_helper import get_db 
from sgx_scraper.utils.json_helper import write_json
from sgx_scraper.utils.sgx_announcement_html import resolve_annual_report
from sgx_scraper.utils.symbol_matching_helper import symbol_from_company_name 

import logging


LOGGER = logging.getLogger(__name__)


def filter_lookup_announcements(
    raw_announcement_api: list,
    is_descending: bool = True,
    is_print_log_matching: bool = False
) -> None:
    primary_symbols = {
        "YANGZIJIANG SHIPBUILDING (HOLDINGS) LTD.": "BS6",
    }

    sorted_records = sorted(
        raw_announcement_api,
        key=lambda announcement: announcement.get("submission_date") or "",
        reverse=is_descending,
    )
    
    top_200 = get_db(
        table="sgx_company_report",
        columns="name,symbol,market_cap",
        query=lambda query: (
            query
            .not_.is_('market_cap', 'null')
            .order('market_cap', desc=True)
            .limit(200)
        ),
    )

    top_200_symbols = {
        record["symbol"]
        for record in top_200
    }

    announcement_lookup = defaultdict(list)
    resolved_symbols_by_issuer_name = {}

    for record in sorted_records:
        issuer_name = record.get("issuer_name")

        issuer_symbols = {
            issuer.get("stock_code")
            for issuer in record.get("issuers") or []
            if issuer.get("stock_code") in top_200_symbols
        }

        if len(issuer_symbols) == 1:
            symbol = next(iter(issuer_symbols))
            
        elif issuer_symbols:
            symbol = primary_symbols.get(issuer_name)

        else:
            if issuer_name not in resolved_symbols_by_issuer_name:
                resolved_symbols_by_issuer_name[issuer_name] = (
                    symbol_from_company_name(
                        issuer_name, 
                        is_print_log=is_print_log_matching
                    )
                )

            symbol = resolved_symbols_by_issuer_name[issuer_name]

        if symbol not in top_200_symbols:
            continue

        announcement_lookup[symbol].append(record)

    LOGGER.info(
        "Total annual report filtered to top 200 lookup: %d", 
        len(announcement_lookup)
    )

    return announcement_lookup


def filter_annual_report_url(
    announcement_lookup: dict,
    output_path: str 
) -> dict:
    pdf_urls = {}

    for index, (symbol, records) in enumerate(
        announcement_lookup.items(),
        start=1 
    ): 
        LOGGER.info(
            "Processing filter anual report url: %d/%d | symbol: %s",
            index, 
            len(announcement_lookup),
            symbol
        )

        pdf_url = None

        for record in records:
            url = record["url"]
            submission_date = record["submission_date"]

            pdf_url, _ = resolve_annual_report(
                url,
                company_name=record.get("security_name"),
            )

            if pdf_url is not None: 
                break 

        pdf_urls[symbol] = {
            "symbol": symbol, 
            "pdf_url": pdf_url, 
            "submission_date": submission_date
        }

        write_json(
            path=output_path, 
            payload=pdf_urls
        )

    LOGGER.info("Total annual report urls covered: %d", len(pdf_urls))

    return pdf_urls
