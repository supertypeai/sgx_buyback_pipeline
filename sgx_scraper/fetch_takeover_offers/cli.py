from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.news.builder import generate_news
from sgx_scraper.utils.cli_helper import push_to_db, filter_top_n_companies
from .parser import get_takeover

import typer
import logging 
import json 


app = typer.Typer(help="Takeover offers scraper pipeline")


@app.command(name="takeover")
def run_sgx_buyback_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    top_n: int = typer.Option(200, help="Only process the top N companies by market cap"),
    is_push_db: bool = typer.Option(True, help="Flag to push to db or not"),
    is_proxy: bool = typer.Option(None, help="Flag to use proxy or not"),
):
    logger = logging.getLogger(__name__)

    payload = []

    announcements = iter_sgx_announcements(
        sub_category="ANNC27",
        flag_log="Takeover",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    )

    logger.info("Total length scraped: %d", len(announcements))

    for index, announcement in enumerate(announcements, start=1):
        url = announcement.get("url")

        logger.info(
            "Processing %d/%d | url: %s", 
            index, 
            len(announcements), 
            url
        )

        result = get_takeover(url=url)

        if result is None:
            logger.warning(
                "Skipping takeover announcement because parsing failed: %s",
                url,
            )
            continue

        payload.append(result)

    if not payload:
        logger.info("Payload is None, stopping")
        return     

    top_n_payload, _ = filter_top_n_companies(payload, top_n=top_n)

    news_payload = generate_news(
        payload=top_n_payload,
        generate_type="takeover"
    )

    logger.info(
        "Generated takeover news:\n%s",
        json.dumps(news_payload, indent=2, ensure_ascii=False),
    )

    if is_push_db:
        push_to_db(news_payload, 'sgx_news')


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )
   
    app()
