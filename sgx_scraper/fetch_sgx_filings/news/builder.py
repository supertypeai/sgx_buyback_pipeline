from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from sgx_scraper.fetch_sgx_filings.llm.client import get_llm 
from sgx_scraper.utils.json_helper import open_json
from sgx_scraper.fetch_sgx_filings.llm.prompts import PromptCollections, TitleBodyGeneration
from .utils.formatter import (
    format_filing_input, 
    format_upcoming_dividend_input, 
    format_takeover_input
)

import logging
import time
import random 


LOGGER = logging.getLogger(__name__)



def generate_news_title_body(
    formatted_record: str, 
    generate_type: str = "filing" 
) -> tuple[str, str] | None:
    generation_parser = JsonOutputParser(pydantic_object=TitleBodyGeneration)
    format_instructions = generation_parser.get_format_instructions()

    prompt_collections = PromptCollections()

    if generate_type == "filing":
        system_prompt = prompt_collections.get_news_filing_system_prompt()
        user_prompt = prompt_collections.get_news_filing_user_prompt()

    elif generate_type == "upcoming_dividend": 
        system_prompt = prompt_collections.get_upcoming_dividend_system_prompt()
        user_prompt = prompt_collections.get_upcoming_dividend_user_prompt()

    elif generate_type == "takeover":
        system_prompt = prompt_collections.get_takeover_system_prompt()
        user_prompt = prompt_collections.get_takeover_user_prompt()

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ('user', user_prompt )
    ])

    for model in ['gpt-oss-120b']:
        try:
            llm = get_llm(model, temperature=0.4)
            LOGGER.info(f"LLM used for news: {model}")

            input_data = {
                "data": formatted_record,
                "format_instructions": format_instructions,
            }

            llm_chain = prompt | llm | generation_parser

            response = llm_chain.invoke(input_data)

            if response is None:
                LOGGER.warning("API call failed after all retries, trying next LLM")
                continue

            if not response.get("title") or not response.get("body"):
                LOGGER.info("LLM news returned incomplete result")
                continue
            
            return response.get('title'), response.get('body')

        except Exception as error:
            LOGGER.warning(f"LLM failed with error: {error}", exc_info=True)
            continue  

    LOGGER.error("All LLMs failed to return a valid generation for news")
    return None, None


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

    if generate_type == "filing":
        tag = ["Insider Trading"] 

    elif generate_type == "upcoming_dividend": 
        tag = ["Dividend Announcement"]

    elif generate_type == "takeover": 
        tag = ["Mergers & Acquisitions"]

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
        if generate_type == "filing":
            formatted_current_data = format_filing_input(record)

        elif generate_type == "upcoming_dividend": 
            formatted_current_data = format_upcoming_dividend_input(record)

        elif generate_type == "takeover": 
            formatted_current_data = format_takeover_input(record)

        result = generate_news_title_body(
            formatted_record=formatted_current_data,
            generate_type=generate_type
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
            generate_type
        )

        results.append(cleaned)

    return results


if __name__ == "__main__": 
    payload = [{
    "symbol": "5TJ",
    "reference": "SG260824DVCA4ZDD",
    "recording_date": "2026-09-01",
    "ex_date": "2026-08-31",
    "dividend_amount": 0.0018,
    "payment_date": "2026-09-08",
    "payment_type": "Tax Exempted (1-tier)",
    "dividend_type": "Final",
    "event_narrative": {
      "narrative": None,
      "information_conditions": None
    },
    "source": "https://links.sgx.com/1.0.0/corporate-announcements/TNYFSTLQ3PZTUQZ4/92b1fd8514f58f3c54dc8f3cd4e3617c6fd6af337a82115ed1f6c88bfba324e1",
    "timestamp": "2026-08-24 18:58:36",
    "updated_on": "2026-08-28 08:54:30"
  }]

    result = generate_news(
        payload=payload,
        generate_type="upcoming_dividend"
    )

    print(result)
