from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from pathlib import Path

from sgx_scraper.fetch_sgx_filings.llm.client import get_llm 
from sgx_scraper.fetch_sgx_filings.llm.prompts import *

import logging
import time
import random 
import json 
import re 


LOGGER = logging.getLogger(__name__)

SLUG_PATTERN = re.compile(r"[^A-Za-z0-9]+")


def fmt_int(value) -> str:
    return f'{value:,}' if value is not None else '-'


def fmt_sgd(value) -> str:
    return f'SGD {value:,}' if value is not None else '-'


def to_kebab(value: str | None) -> str:
    if not value:
        return "unknown"
    
    return SLUG_PATTERN.sub("-", value.strip()).strip("-").lower()


def format_filing_for_prompt(filing: dict) -> str:
    lines = [
        f"symbol: {filing.get('symbol') or '-'}",
        f"company name: {filing.get('issuer_name') or '-'}",
        f"holder name: {filing.get('holder_name') or '-'}",
        f"holder type: {filing.get('holder_type') or '-'}",
        f"transaction type: {filing.get('transaction_type') or '-'}",
        f"shares transacted: {fmt_int(filing.get('amount_transaction'))}",
        f"transaction value: {fmt_sgd(filing.get('transaction_value'))}",
        f"price_per_share: {fmt_sgd(filing.get('price_per_share'))}",
        f"holding before: {fmt_int(filing.get('holding_before'))}",
        f"holding after: {fmt_int(filing.get('holding_after'))}",
        f"ownership before: {filing.get('share_percentage_before')}",
        f"ownership after: {filing.get('share_percentage_after')}",
        f"timestamp: {filing.get('timestamp') or '-'}",
    ]
    return '\n'.join(lines)


def generate_news_title_body(record: dict) -> tuple[str, str] | None:
    generation_parser = JsonOutputParser(pydantic_object=TitleBodyGeneration)
    format_instructions = generation_parser.get_format_instructions()

    prompt_collections = PomptCollections()
    system_prompt = prompt_collections.get_system_prompt()
    user_prompt = prompt_collections.get_user_prompt()

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ('user', user_prompt )
    ])

    for model in ['gpt-oss-120b']:
        try:
            llm = get_llm(model, temperature=0.4)
            LOGGER.info(f"LLM used for news: {model}")

            formatted_current_filing =  format_filing_for_prompt(record)

            input_data = {
                'current_filing': formatted_current_filing,
                'format_instructions': format_instructions,
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
) -> dict:
    companies_path = Path('data/sgx_companies.json')

    with companies_path.open('r') as file: 
        companies = json.load(file)

    symbol = record.get('symbol', '')
    
    sector = companies.get(symbol).get('sector')
    sub_sector = companies.get(symbol).get('sub_sector')

    return {
        'title': title,
        'body': body,
        'source': record.get('source'),
        'timestamp': record.get('timestamp'),
        'sector': to_kebab(sector),
        'sub_sector': [to_kebab(sub_sector)],
        'tags': ['Insider Trading'],
        'tickers': [symbol],
        'symbols': [symbol],
        'dimension': None,
        'votes': None,
        'score': None,
    }


def generate_news(payload: list[dict]) -> list[dict]:
    results = []

    for record in payload:
        result = generate_news_title_body(record)
        time.sleep(random.randint(2, 5))

        if result is None:
            LOGGER.warning(f"Skipping news generation for {record.get('symbol')} — all LLMs failed")
            continue

        title, body = result
        cleaned = clean_news_payload(record, title, body)
        results.append(cleaned)

    return results


