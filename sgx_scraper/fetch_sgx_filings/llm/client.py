from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatResult
from langchain_core.callbacks import BaseCallbackHandler

from sgx_scraper.config.settings import GROQ_API_KEY, OPENROUTER_API_KEY
from sgx_scraper.utils.constant import (
    MODEL_CONFIG, 
    ABORT_KEYWORDS, 
    ABORT_STATUS_CODES, 
    LLM_TIMEOUT_SECONDS,
    OPENROUTER_BASE_URL,
    ROTATE_400_KEYWORDS, 
    ROTATE_BACKOFF_SECONDS,
    ROTATE_KEYWORDS, 
    ROTATE_MAX_SWEEPS,
    ROTATE_STATUS_CODES
)

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

import asyncio
import logging
import time


LOGGER = logging.getLogger(__name__)


class TokenUsageLogger(BaseCallbackHandler):
    def on_llm_end(self, response, **kwargs):
        token_usage = response.llm_output.get("token_usage", {}) if response.llm_output else {}
        completion_details = token_usage.get("completion_tokens_details") or {}

        LOGGER.info(
            "token usage: prompt=%d completion=%d reasoning=%d total=%d finish_reason=%s",
            token_usage.get("prompt_tokens", 0),
            token_usage.get("completion_tokens", 0),
            completion_details.get("reasoning_tokens", 0),
            token_usage.get("total_tokens", 0),
            response.generations[0][0].generation_info.get("finish_reason", "unknown")
            if response.generations else "unknown",
        )


def extract_status_code(error: Exception) -> int | None:
    status_code = getattr(error, "status_code", None)
    if status_code is not None:
        return int(status_code)

    for token in str(error).split():
        if token.isdigit() and len(token) == 3:
            return int(token)

    return None


def classify_error(error: Exception) -> str:
    """
    Returns one of three actions:
      'rotate' -> key-level problem, try the next key
      'abort'  -> request-level or server-level problem, rotating will not help
      'raise'  -> unexpected error, propagate immediately
    """
    status_code = extract_status_code(error)
    error_message = str(error).lower()

    if status_code == 400 and any(keyword in error_message for keyword in ROTATE_400_KEYWORDS):
        return "rotate"
    
    if status_code in ROTATE_STATUS_CODES:
        return "rotate"

    if status_code in ABORT_STATUS_CODES:
        return "abort"

    if any(keyword in error_message for keyword in ROTATE_KEYWORDS):
        return "rotate"

    if any(keyword in error_message for keyword in ABORT_KEYWORDS):
        return "abort"

    return "raise"


class KeyRotatingChatModel(BaseChatModel):
    """
    Wraps a pool of LLM clients initialised with different API keys for the
    same model. On a key-level failure (429, 401, 403) it transparently
    rotates to the next available key. On request-level or server-level
    failures it raises immediately without wasting the remaining keys.
    """
    llm_pool: list[BaseChatModel]
    model_name_identifier: str

    class Config:
        arbitrary_types_allowed = True

    @property
    def _llm_type(self) -> str:
        return f"key-rotating-{self.model_name_identifier}"

    def handle_error(self, error: Exception, index: int) -> str:
        action = classify_error(error)

        if action == "rotate":
            LOGGER.warning(
                f"Key index {index} failed for '{self.model_name_identifier}' "
                f"(rotating to next key). Error: {error}"
            )
            return action

        LOGGER.error(
            f"Non-recoverable error for '{self.model_name_identifier}', "
            f"aborting key rotation. Error: {error}"
        )
        return "raise"

    def call_with_deadline(self, llm_client, messages, stop, **kwargs):
        """The openrouter SDK exposes no timeout, so it is enforced here."""
        executor = ThreadPoolExecutor(max_workers=1)

        try:
            future = executor.submit(llm_client._generate, messages, stop=stop, **kwargs)
            return future.result(timeout=LLM_TIMEOUT_SECONDS)

        finally:
            executor.shutdown(wait=False)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        **kwargs: any,
    ) -> ChatResult:
        last_error: Exception | None = None

        for sweep in range(ROTATE_MAX_SWEEPS):
            if sweep:
                backoff = ROTATE_BACKOFF_SECONDS * sweep
                LOGGER.warning(
                    f"All keys rate-limited for '{self.model_name_identifier}', "
                    f"retrying the pool in {backoff}s"
                )
                time.sleep(backoff)

            for index, llm_client in enumerate(self.llm_pool):
                try:
                    return self.call_with_deadline(llm_client, messages, stop, **kwargs)

                except FutureTimeout:
                    LOGGER.warning(
                        f"Key index {index} exceeded {LLM_TIMEOUT_SECONDS}s for "
                        f"'{self.model_name_identifier}', abandoning the call"
                    )
                    last_error = RuntimeError(
                        f"no reply within {LLM_TIMEOUT_SECONDS}s"
                    )

                except Exception as error:
                    if self.handle_error(error, index) == "raise":
                        raise

                    last_error = error

        raise RuntimeError(
            f"All {len(self.llm_pool)} API keys exhausted for model "
            f"'{self.model_name_identifier}'. Last error: {last_error}"
        )

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        **kwargs: any,
    ) -> ChatResult:
        last_error: Exception | None = None

        for sweep in range(ROTATE_MAX_SWEEPS):
            if sweep:
                backoff = ROTATE_BACKOFF_SECONDS * sweep
                LOGGER.warning(
                    f"All keys rate-limited for '{self.model_name_identifier}' "
                    f"(async), retrying the pool in {backoff}s"
                )
                await asyncio.sleep(backoff)

            for index, llm_client in enumerate(self.llm_pool):
                try:
                    return await llm_client._agenerate(messages, stop=stop, **kwargs)

                except Exception as error:
                    if self.handle_error(error, index) == "raise":
                        raise

                    last_error = error

        raise RuntimeError(
            f"All {len(self.llm_pool)} API keys exhausted for model "
            f"'{self.model_name_identifier}'. Last error: {last_error}"
        )
    

def get_llm(
    model_name: str,
    temperature: float = 0.5,
    max_retries: int = 3,
):
    config_model = MODEL_CONFIG.get(model_name)

    if config_model is None:
        available_models = ', '.join(MODEL_CONFIG.keys())
        LOGGER.error(f"Unknown model name: '{model_name}'. Available models: {available_models}")
        return None
    
    provider = config_model.get('provider')

    provider_keys = {
        'groq': [GROQ_API_KEY],
        'openrouter': [OPENROUTER_API_KEY]
    }

    api_keys = [
        key 
        for key in provider_keys.get(provider, []) 
        if key
    ]

    # the native openrouter provider ignores timeout and never returns on a stall
    is_openrouter = provider == 'openrouter'
    
    if not api_keys:
        LOGGER.error(f"No valid API keys found for provider: '{provider}'")
        return None
    
    llm_pool = []
    
    for api_key in api_keys:
        try:
            initiate_model = init_chat_model(
                config_model.get('model'),
                model_provider='openai' if is_openrouter else provider,
                temperature=temperature,
                max_retries=max_retries,
                api_key=api_key,
                max_tokens=10000,
                timeout=LLM_TIMEOUT_SECONDS,
                **({'base_url': OPENROUTER_BASE_URL} if is_openrouter else {}),
            ) 

            llm_pool.append(initiate_model)
    
        except Exception as error:
            LOGGER.error(f'Error initialize llm: {error}')
            continue 
    
    if not llm_pool:
        LOGGER.error(f"No clients could be initialized for '{model_name}'")
        return None

    return KeyRotatingChatModel(llm_pool=llm_pool, model_name_identifier=model_name)
