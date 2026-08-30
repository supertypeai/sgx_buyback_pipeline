from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatResult
from langchain_core.callbacks import BaseCallbackHandler

from sgx_scraper.config.settings import GROQ_API_KEY, OPENROUTER_API_KEY
from sgx_scraper.llm.constants import (
    MODEL_CONFIG, 
    ABORT_KEYWORDS, 
    ABORT_STATUS_CODES, 
    ROTATE_400_KEYWORDS, 
    ROTATE_KEYWORDS, 
    ROTATE_STATUS_CODES
)

import logging 


LOGGER = logging.getLogger(__name__)


class TokenUsageLogger(BaseCallbackHandler):
    def on_llm_end(self, response, **kwargs):
        llm_output = response.llm_output or {}
        token_usage = llm_output.get("token_usage") or {}

        if not token_usage and response.generations:
            generation = response.generations[0][0]
            message = getattr(generation, "message", None)
            token_usage = getattr(message, "usage_metadata", None) or {}

        completion_details = token_usage.get("completion_tokens_details") or {}
        output_details = token_usage.get("output_token_details") or {}

        prompt_tokens = token_usage.get(
            "prompt_tokens",
            token_usage.get("input_tokens", 0),
        )

        completion_tokens = token_usage.get(
            "completion_tokens",
            token_usage.get("output_tokens", 0),
        )

        reasoning_tokens = completion_details.get(
            "reasoning_tokens",
            output_details.get("reasoning", 0),
        )

        total_tokens = token_usage.get("total_tokens", 0)

        generation_info = (
            response.generations[0][0].generation_info
            if response.generations
            else {}
        ) or {}

        LOGGER.info(
            "token usage: model=%s prompt=%d completion=%d reasoning=%d "
            "total=%d finish_reason=%s",
            llm_output.get("model_name", "unknown"),
            prompt_tokens,
            completion_tokens,
            reasoning_tokens,
            total_tokens,
            generation_info.get("finish_reason", "unknown"),
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

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        **kwargs: any,
    ) -> ChatResult:
        last_error: Exception | None = None

        for index, llm_client in enumerate(self.llm_pool):
            try:
                return llm_client._generate(messages, stop=stop, **kwargs)
            
            except Exception as error:
                action = classify_error(error)

                if action == "rotate":
                    LOGGER.warning(
                        f"Key index {index} failed for '{self.model_name_identifier}' "
                        f"(rotating to next key). Error: {error}"
                    )
                    last_error = error
                    continue

                if action == "abort":
                    LOGGER.error(
                        f"Non-recoverable error for '{self.model_name_identifier}', "
                        f"aborting key rotation. Error: {error}"
                    )
                    raise

                raise

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

        for index, llm_client in enumerate(self.llm_pool):
            try:
                return await llm_client._agenerate(messages, stop=stop, **kwargs)
            
            except Exception as error:
                action = classify_error(error)

                if action == "rotate":
                    LOGGER.warning(
                        f"Key index {index} failed for '{self.model_name_identifier}' "
                        f"(async, rotating to next key). Error: {error}"
                    )
                    last_error = error
                    continue

                if action == "abort":
                    LOGGER.error(
                        f"Non-recoverable error for '{self.model_name_identifier}' "
                        f"(async), aborting key rotation. Error: {error}"
                    )
                    raise

                raise

        raise RuntimeError(
            f"All {len(self.llm_pool)} API keys exhausted for model "
            f"'{self.model_name_identifier}'. Last error: {last_error}"
        )
    

def get_llm(
    model_name: str,
    temperature: float = 0.5,
    effort: str = "low",
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
    
    if not api_keys:
        LOGGER.error(f"No valid API keys found for provider: '{provider}'")
        return None
    
    llm_pool = []
    
    for api_key in api_keys:
        try:
            model_parameters = {
                "temperature": temperature,
                "max_retries": max_retries,
                "api_key": api_key,
                "max_tokens": 16000 if model_name == "nvidia-nemotron-3-ultra" else 25000,
            }

            if provider == "openrouter":
                model_parameters["reasoning"] = {
                    "effort": effort,
                }

            elif provider == "groq" and effort != "none":
                model_parameters["reasoning_effort"] = effort

            initiate_model = init_chat_model(
                config_model.get('model'),
                model_provider=provider,
                **model_parameters,
            ) 

            llm_pool.append(initiate_model)
    
        except Exception as error:
            LOGGER.error(f'Error initialize llm: {error}')
            continue 
    
    if not llm_pool:
        LOGGER.error(f"No clients could be initialized for '{model_name}'")
        return None

    return KeyRotatingChatModel(
        llm_pool=llm_pool,
        model_name_identifier=model_name,
        callbacks=[TokenUsageLogger()],
    )
