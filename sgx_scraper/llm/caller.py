from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from pydantic import BaseModel
from pathlib import Path

from sgx_scraper.llm.client import get_llm

import asyncio
import logging 
import time 
import base64


LOGGER = logging.getLogger(__name__)


def encode_image(image_path: str) -> str:
    image_bytes = Path(image_path).read_bytes()
    return base64.b64encode(image_bytes).decode("utf-8")


DEFAULT_MODELS = [
    "gpt-6-luna",
    "gpt-oss-120b",
    "nvidia-nemotron-3-ultra",
    "deepsek-v4-flash",
]


def build_structured_input(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    input_data: dict[str],
) -> tuple:
    parser = JsonOutputParser(
        pydantic_object=pydantic_output
    )

    prompt = ChatPromptTemplate.from_messages([
        ('system', system_prompt),
        ('user', user_prompt),
    ])

    input_data = {
        **input_data,
        "format_instructions": parser.get_format_instructions(),
    }

    return prompt, parser, input_data


def unwrap_extraction(extraction, model: str) -> dict | None:
    # the model sometimes wraps the object in a JSON array, unwrap to a dict
    if isinstance(extraction, list):
        extraction = next(
            (
                item
                for item in extraction
                if isinstance(item, dict)
            ),
            None
        )

    if not isinstance(extraction, dict):
        LOGGER.warning(
            "[fallback] %s returned non-object extraction, skipping",
            model
        )
        return None

    return extraction


def invoke_structured_llm(
    pydantic_output: type[BaseModel], 
    system_prompt: str, 
    user_prompt: str,
    log_name: str,
    input_data: dict[str], 
    models: list[str] = DEFAULT_MODELS,
    max_retry: int = 3,
    temperature: int = 0.3, 
    effort: str = "high"
) -> dict | list | None:
    prompt, parser, input_data = build_structured_input(
        pydantic_output, system_prompt, user_prompt, input_data
    )

    for model in models:
        attempt = 1 

        while attempt <= max_retry:
            try:
                llm = get_llm(
                    model, 
                    temperature=temperature,
                    effort=effort
                )

                if llm is None:
                    attempt += 1
                    continue

                LOGGER.info(
                    "[LLM Caller][%s] model used %s (attempt %d/%d)", 
                    log_name,
                    model, 
                    attempt, 
                    max_retry
                )

                extraction = (prompt | llm | parser).invoke(input_data)

                LOGGER.info(
                    "[LLM Caller] raw extraction: %s", 
                    extraction
                )

                extraction = unwrap_extraction(extraction, model)

                if extraction is None:
                    continue

                return extraction

            except Exception as error:
                LOGGER.warning(
                    "[fallback] model %s failed: %s", 
                    model, 
                    error
                )

            attempt += 1

            if attempt <= max_retry:
                time.sleep(2.5)      

    return None


async def ainvoke_structured_llm(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    log_name: str,
    input_data: dict[str],
    models: list[str] = DEFAULT_MODELS,
    max_retry: int = 3,
    temperature: int = 0.3,
    effort: str = "high"
) -> dict | None:
    prompt, parser, input_data = build_structured_input(
        pydantic_output, system_prompt, user_prompt, input_data
    )

    for model in models:
        for attempt in range(1, max_retry + 1):
            llm = get_llm(
                model,
                temperature=temperature,
                effort=effort
            )

            if llm is None:
                continue

            try:
                extraction = await (prompt | llm | parser).ainvoke(input_data)
            except Exception as error:
                LOGGER.warning(
                    "[LLM Caller][%s] model %s failed (attempt %d/%d): %s",
                    log_name,
                    model,
                    attempt,
                    max_retry,
                    error
                )
                await asyncio.sleep(2.5)
                continue

            extraction = unwrap_extraction(extraction, model)

            if extraction is not None:
                return extraction

    return None
