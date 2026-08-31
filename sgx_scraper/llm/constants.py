MODEL_CONFIG = {
    "nvidia-nemotron-3-ultra": {
        "model": "nvidia/nemotron-3-ultra-550b-a55b:free",
        "provider": "openrouter",
    },
    "deepsek-v4-flash": {
        "model": "~deepseek/deepseek-v4-flash-latest",
        "provider": "openrouter",
    },
    "qwen-3.5-flash": {
        "model": "qwen/qwen3.5-flash-02-23",
        "provider": "openrouter"
    },
    "gpt-oss-120b": {
        "model": "openai/gpt-oss-120b",
        "provider": "groq",
    },
    "laguna-s-2.1": {
        "model": "poolside/laguna-s-2.1:free",
        "provider": "openrouter"
    }
}

ROTATE_STATUS_CODES = {401, 403, 429, 413}
ABORT_STATUS_CODES = {400, 422, 500, 502, 503, 504}

ROTATE_KEYWORDS = (
    "rate limit",
    "too many requests",
    "authentication",
    "invalid api key",
    "request too large",
)
ROTATE_400_KEYWORDS = ("organization_restricted",)

ABORT_KEYWORDS = (
    "context length",
    "max token",
    "internal server",
    "bad gateway",
    "service unavailable",
)
