from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.messages import HumanMessage, SystemMessage

from pydantic import BaseModel
from pathlib import Path

from sgx_scraper.llm.client import get_llm

import logging 
import time 
import base64
import mimetypes


LOGGER = logging.getLogger(__name__)


def encode_image(image_path: str) -> str:
    image_bytes = Path(image_path).read_bytes()
    return base64.b64encode(image_bytes).decode("utf-8")


def invoke_structured_llm(
    pydantic_output: type[BaseModel], 
    system_prompt: str, 
    user_prompt: str,
    log_name: str,
    input_data: dict[str], 
    models: list[str] = [
        "nvidia-nemotron-3-ultra", 
        "deepsek-v4-flash",
        "gpt-oss-120b"
    ],
    max_retry: int = 3,
    temperature: int = 0.3, 
    effort: str = "low"
) -> dict | None:
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


def invoke_structured_vision_llm(
    pydantic_output: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    image_path: str,
    log_name: str,
    models: list[str] | None = ["qwen-3.5-flash"],
    max_retry: int = 3,
    temperature: float = 0.3,
    effort: str = "low",
) -> dict | None:
    image_file = Path(image_path)

    if not image_file.exists():
        LOGGER.error(
            "[Vision LLM Caller][%s] image does not exist: %s",
            log_name,
            image_path,
        )
        return None

    mime_type, _ = mimetypes.guess_type(image_file.name)

    if mime_type not in {
        "image/png",
        "image/jpeg",
        "image/webp",
    }:
        LOGGER.error(
            "[Vision LLM Caller][%s] unsupported image type: %s",
            log_name,
            mime_type,
        )
        return None

    image_base64 = base64.b64encode(
        image_file.read_bytes()
    ).decode("utf-8")

    parser = JsonOutputParser(
        pydantic_object=pydantic_output,
    )

    formatted_user_prompt = (
        f"{user_prompt}\n\n"
        f"{parser.get_format_instructions()}"
    )

    messages = [
        SystemMessage(
            content=system_prompt,
        ),
        HumanMessage(
            content=[
                {
                    "type": "text",
                    "text": formatted_user_prompt,
                },
                {
                    "type": "image",
                    "base64": image_base64,
                    "mime_type": mime_type,
                },
            ]
        ),
    ]

    for model in models:
        for attempt in range(1, max_retry + 1):
            try:
                llm = get_llm(
                    model,
                    temperature=temperature,
                    effort=effort,
                )

                if llm is None:
                    LOGGER.warning(
                        "[Vision LLM Caller][%s] model unavailable: %s",
                        log_name,
                        model,
                    )
                    break

                LOGGER.info(
                    "[Vision LLM Caller][%s] model used %s "
                    "(attempt %d/%d)",
                    log_name,
                    model,
                    attempt,
                    max_retry,
                )

                extraction = (
                    llm | parser
                ).invoke(messages)

                LOGGER.info(
                    "[Vision LLM Caller][%s] raw extraction: %s",
                    log_name,
                    extraction,
                )

                if isinstance(extraction, list):
                    extraction = next(
                        (
                            item
                            for item in extraction
                            if isinstance(item, dict)
                        ),
                        None,
                    )

                if not isinstance(extraction, dict):
                    LOGGER.warning(
                        "[Vision LLM Caller][%s] %s returned "
                        "non-object extraction",
                        log_name,
                        model,
                    )
                    continue

                return extraction

            except Exception as error:
                LOGGER.warning(
                    "[Vision LLM Caller][%s] model %s failed: %s",
                    log_name,
                    model,
                    error,
                )

            if attempt < max_retry:
                time.sleep(2.5)

    return None
