from collections import defaultdict

from sgx_scraper.utils.cli_helper import get_db 
from sgx_scraper.utils.json_helper import open_json, write_json
from sgx_scraper.utils.sgx_announcement_html import resolve_annual_report
from sgx_scraper.utils.symbol_matching_helper import symbol_from_company_name 

import logging


LOGGER = logging.getLogger(__name__)


def filter_lookup_announcements(
    path: str = "sgx_scraper/fetch_managements/announcements.json",
    output_path: str = (
        "sgx_scraper/fetch_managements/"
        "cleaned_announcements_appointment_cessation.json"
    ),
    sort_oldest_first: bool = False,
) -> None:
    primary_symbols = {
        "YANGZIJIANG SHIPBUILDING (HOLDINGS) LTD.": "BS6",
    }

    records = open_json(path)

    sorted_records = sorted(
        records,
        key=lambda announcement: announcement.get("submission_date") or "",
        reverse=not sort_oldest_first,
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
                    symbol_from_company_name(issuer_name)
                )

            symbol = resolved_symbols_by_issuer_name[issuer_name]

        if symbol not in top_200_symbols:
            continue

        announcement_lookup[symbol].append(record)

    write_json(
        output_path,
        dict(announcement_lookup)
    )

    LOGGER.info("length cleaned: %d", len(announcement_lookup))


def filter_annual_report_url(
    cleaned_announcements_path: str, 
    output_path: str 
) -> None:
    announcements = open_json(cleaned_announcements_path) 

    pdf_urls = []

    for symbol, records in announcements.items(): 
        pdf_url = None

        for record in records:
            url = record["url"]

            pdf_url, resolved_company_name = resolve_annual_report(
                url,
                company_name=record.get("security_name") or resolved_company_name,
            )

            if pdf_url is not None: 
                break 

        pdf_urls.append({
            "symbol": symbol, 
            "pdf_url": pdf_url
        })

        write_json(
            path=output_path, # "sgx_scraper/fetch_managements/pdf_urls.json", 
            payload=pdf_urls
        )

    LOGGER.info("Total annual report urls covered: %d", len(pdf_urls))
