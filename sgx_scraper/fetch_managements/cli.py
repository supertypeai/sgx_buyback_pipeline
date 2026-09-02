from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import upsert_to_db, push_to_db
from sgx_scraper.alerting.mailer import send_sgx_alert_email
from sgx_scraper.alerting.build_template import render_management_email_content
from sgx_scraper.fetch_managements.management import (
    search_appointed_date_with_tavily,
    get_management_payload, 
    enrich_management_records
)
from sgx_scraper.utils.json_helper import write_json, open_json
from sgx_scraper.fetch_managements.tracking import (
    get_management_update,
)
from .process_annual_report import (
    filter_annual_report_url, 
    filter_lookup_announcements
)
from sgx_scraper.news.builder import generate_news
from sgx_scraper.utils.cli_helper import get_db

import typer
import logging


LOGGER = logging.getLogger(__name__)

app = typer.Typer(help="SGX management pipeline")


@app.command(name='track_management')
def run_tracking_management(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    payload_management_by_symbol = {}
    management_news_payload = []

    announcements = list(iter_sgx_announcements(
        sub_category="ANNC03%2CANNC04",
        flag_log="Management",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    ))

    top_200 = get_db(
        table="sgx_company_report",
        columns="symbol",
        query=lambda query: (
            query
            .not_.is_("market_cap", "null")
            .order("market_cap", desc=True)
            .limit(200)
        ),
    )

    top_200_symbols = {
        record["symbol"]
        for record in top_200
    }

    companies = {
        symbol: record
        for symbol, record in open_json("data/sgx_companies.json").items()
        if symbol in top_200_symbols
    }

    announcements = sorted(
        announcements,
        key=lambda announcement: announcement.get("submission_date_time") or 0,
    )

    for announcement in announcements:
        try:
            # build management and former management 
            result = get_management_update(
                api_response=announcement,
                management_by_symbol=companies
            )

            if not result:
                continue

            management_record = result["management_record"]
            management_announcement = result["announcement"]
            symbol = management_record["symbol"]

            payload_management_by_symbol[symbol] = management_record
            management_news_payload.append(management_announcement)

        except Exception as error:
            LOGGER.error(
                "[TRACKING MANAGEMENT] Error processing announcement: %s",
                error, 
                exc_info=True
            )
            continue

    payload_management = list(payload_management_by_symbol.values())

    if not payload_management:
        LOGGER.info("[TRACKING MANAGEMENT] No payload generated, stopping.")
        return

    LOGGER.info(
        "Total unique management payloads to upsert: %d",
        len(payload_management)
    )

    LOGGER.info(
        "payload management to upsert: %s",
        payload_management
    )

    news_payload = generate_news(
        payload=management_news_payload,
        generate_type="management",
    )

    LOGGER.info(
        "Generated management news records: %d",
        len(news_payload),
    )

    LOGGER.info(news_payload)

    if is_push_db:
        push_to_db(
            payload=news_payload,
            table_name="sgx_news",
        )

        upsert_to_db(
            payload=payload_management, 
            table_name="sgx_companies",
            on_conflict="symbol"
        )


@app.command(name='scraper_managements')
def run_managements_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_upsert_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    announcements = list(
        iter_sgx_announcements(
            sub_category="ANNC30",
            flag_log="Annual Reports",
            period_start=period_start,
            period_end=period_end,
            page_size=page_size,
            is_proxy=is_proxy,
        )
    )

    base_dir = "data/scraper_output/sgx_managements/annual_report"
    clean_url_output_path = f"{base_dir}/processed/new/clean_annual_report_urls.json"

    lookup_top_200 = filter_lookup_announcements(
        raw_announcement_api=announcements, 
        is_descending=True
    )

    new_pdf_urls = filter_annual_report_url(
        announcement_lookup=lookup_top_200,
        output_path=clean_url_output_path
    )

    historical_urls = open_json(
        f"{base_dir}/processed/historical/annual_report_urls.json"
    )

    companies = open_json("data/sgx_companies.json")

    final_payload = []
    management_alerts = []

    for symbol, record in new_pdf_urls.items():
        submission_date = record["submission_date"]
        annual_report_url = record["pdf_url"]
        company_name = companies[symbol]["name"]

        historical_record = historical_urls.get(symbol)
        historical_submission_date = historical_record["submission_date"]

        if submission_date <= historical_submission_date:
            LOGGER.info(
                "Submission date is not newest for symbol: %s | url: %s | Skipping",
                symbol, 
                annual_report_url
            )
            continue 

        if not annual_report_url: 
            LOGGER.warning(
                "Annual report URL is missing for symbol: %s",
                symbol,
            )
            continue 
        
        result = get_management_payload(
            annual_report_url=annual_report_url, 
            company_name=company_name
        )

        management_payload = result.get("bod_payload") if result else None

        # fallback search for alternative page bod
        need_fallback = any(
            management_record.get("start_date") is None
            for management_record in management_payload
        )

        if need_fallback:
            LOGGER.info(
                "Running management LLM fallback for symbol: %s",
                symbol,
            )
             
            fallback_result = get_management_payload(
                annual_report_url=annual_report_url,
                models=["deepsek-v4-flash", "nvidia-nemotron-3-ultra"],
                company_name=company_name,
                is_fallback=True,
            )
            
            fallback_payload = (
                fallback_result.get("bod_payload")
                if fallback_result
                else None
            )

            if fallback_payload:
                management_payload = enrich_management_records(
                    primary_records=management_payload,
                    fallback_records=fallback_payload,
                )

        # fallback tavily search
        for management_record in management_payload:
            if management_record.get("start_date") is not None:
                continue

            LOGGER.info(
                (
                    "Searching appointment date with Tavily "
                    "| symbol: %s | name: %s"
                ),
                symbol,
                management_record["name"],
            )

            tavily_start_date = search_appointed_date_with_tavily(
                name=management_record["name"],
                position=management_record["position"],
                company_name=company_name,
            )

            if tavily_start_date is not None:
                management_record["start_date"] = tavily_start_date

        # fallback send email for manual review 
        for management_record in management_payload:
            if management_record.get("start_date") is not None:
                continue

            management_alerts.append({
                "symbol": symbol,
                "company_name": company_name,
                "name": management_record["name"],
                "position": management_record["position"],
                "annual_report_url": annual_report_url,
                "issue": "Unable to determine management start date",
            })

        final_payload.append({
            "symbol": symbol, 
            "management": management_payload
        })

    if not final_payload:
        LOGGER.info("[MANAGEMENT] No payload generated, stopping.")
        return

    if management_alerts:
        LOGGER.warning(
            "%s management records require manual review",
            len(management_alerts),
        )

        subject, body_text, body_html = render_management_email_content(
            alerts=management_alerts,
            title="SGX Management Records Requiring Review",
        )

        send_sgx_alert_email(
            subject=subject,
            body_text=body_text,
            body_html=body_html,
        )

    else:
        LOGGER.info(
            "No SGX management records require manual review."
        )

    if is_upsert_db and final_payload:
        is_upserted = upsert_to_db(
            payload=final_payload, 
            table_name="sgx_companies",
            on_conflict="symbol"
        )

        if not is_upserted:
            LOGGER.error("Management upsert failed: checkpoint was not updated")
            return

        # write back to historical as new checkpoint
        for payload_record in final_payload:
            symbol = payload_record["symbol"]
            historical_urls[symbol] = new_pdf_urls[symbol]

        write_json(
            path=(
                f"{base_dir}/processed/historical/"
                "annual_report_urls.json"
            ),
            payload=historical_urls,
        )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    logging.getLogger('WDM').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire2').setLevel(logging.WARNING)
    logging.getLogger('mitmproxy').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    
    app()
