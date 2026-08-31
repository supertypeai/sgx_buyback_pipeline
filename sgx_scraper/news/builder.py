from sgx_scraper.llm.caller import invoke_structured_llm
from sgx_scraper.utils.json_helper import open_json
from .filing.prompt import (
    SYSTEM_PROMPT as FILING_SYSTEM_PROMPT,
    USER_PROMPT as FILING_USER_PROMPT,
)
from .upcoming_dividend.prompt import (
    SYSTEM_PROMPT as UPCOMING_DIVIDEND_SYSTEM_PROMPT,
    USER_PROMPT as UPCOMING_DIVIDEND_USER_PROMPT,
)
from .takeover.prompt import (
    SYSTEM_PROMPT as TAKEOVER_SYSTEM_PROMPT,
    USER_PROMPT as TAKEOVER_USER_PROMPT,
)
from .management.appointment.prompt import (
    SYSTEM_PROMPT as APPOINTMENT_SYSTEM_PROMPT,
    USER_PROMPT as APPOINTMENT_USER_PROMPT,
)
from .management.cessation.prompt import (
    SYSTEM_PROMPT as CESSATION_SYSTEM_PROMPT,
    USER_PROMPT as CESSATION_USER_PROMPT,
)
from .models import TitleBodyGeneration
from .utils.formatter import (
    format_filing_input, 
    format_upcoming_dividend_input, 
    format_takeover_input,
    format_appointment_input,
    format_cessation_input,
)

import logging
import time
import random 


LOGGER = logging.getLogger(__name__)


NEWS_PROMPT_MAPPING = {
    "filing": (
        FILING_SYSTEM_PROMPT,
        FILING_USER_PROMPT,
    ),
    "upcoming_dividend": (
        UPCOMING_DIVIDEND_SYSTEM_PROMPT,
        UPCOMING_DIVIDEND_USER_PROMPT,
    ),
    "takeover": (
        TAKEOVER_SYSTEM_PROMPT,
        TAKEOVER_USER_PROMPT,
    ),
    "appointment": (
        APPOINTMENT_SYSTEM_PROMPT,
        APPOINTMENT_USER_PROMPT,
    ),
    "cessation": (
        CESSATION_SYSTEM_PROMPT,
        CESSATION_USER_PROMPT,
    ),
}

NEWS_FORMATTER_MAPPING = {
    "filing": format_filing_input,
    "upcoming_dividend": format_upcoming_dividend_input,
    "takeover": format_takeover_input,
    "appointment": format_appointment_input,
    "cessation": format_cessation_input,
}

NEWS_TAG_MAPPING = {
    "filing": ["Insider Trading"],
    "upcoming_dividend": ["Dividend Announcement"],
    "takeover": ["Mergers & Acquisitions"],
    "appointment": ["Executive Changes"],
    "cessation": ["Executive Changes"],
}


def generate_news_title_body(
    formatted_record: str, 
    generate_type: str = "filing" 
) -> tuple[str, str] | None:
    prompt = NEWS_PROMPT_MAPPING.get(generate_type)

    if prompt is None:
        LOGGER.error("Unknown news generation type: %s", generate_type)
        return None

    system_prompt, user_prompt = prompt

    response = invoke_structured_llm(
        pydantic_output=TitleBodyGeneration,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        log_name=f"SGX News {generate_type}",
        input_data={"data": formatted_record},
        models=["gpt-oss-120b"],
        temperature=0.4,
        effort="low",
    )

    if not response:
        LOGGER.warning("LLM news generation returned no response")
        return None

    title = response.get("title")
    body = response.get("body")

    if not title or not body:
        LOGGER.warning("LLM news returned an incomplete result")
        return None

    return title, body


def clean_news_payload(
    record: dict, 
    title: str, 
    body: str,
    generate_type: str = "filing" 
) -> dict:
    companies_path = "data/sgx_companies.json"
    companies = open_json(companies_path)

    symbol = record.get('symbol', '')

    company = companies.get(symbol) or {}
    
    sector = company.get('sector')
    sub_sector = company.get('sub_sector')

    news_type = record.get("category") or generate_type
    tag = NEWS_TAG_MAPPING.get(news_type, [])

    return {
        'title': title,
        'body': body,
        'source': record.get('source'),
        'timestamp': record.get('timestamp'),
        'sector': record.get('sector') or sector,
        'sub_sector': [record.get('sub_sector') or sub_sector],
        'tags': tag,
        'symbols': [symbol],
        'dimension': None,
        'votes': None,
        'score': None,
        'thumbnail': None 
    }


def generate_news(
    payload: list[dict], 
    generate_type: str = "filing"
) -> list[dict]:
    if payload is None or not payload:
        return []
    
    results = []

    for record in payload:
        news_type = record.get("category") or generate_type
        formatter = NEWS_FORMATTER_MAPPING.get(news_type)

        if formatter is None:
            LOGGER.warning(
                "Skipping news generation for unsupported type: %s",
                news_type,
            )
            continue

        formatted_current_data = formatter(record)

        result = generate_news_title_body(
            formatted_record=formatted_current_data,
            generate_type=news_type,
        )   

        time.sleep(random.randint(2, 5))

        if result is None:
            LOGGER.warning(f"Skipping news generation for {record.get('symbol')} — all LLMs failed")
            continue

        title, body = result

        cleaned = clean_news_payload(
            record, 
            title, 
            body,
            news_type,
        )

        results.append(cleaned)

    return results
