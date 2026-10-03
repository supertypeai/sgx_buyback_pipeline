# Built by runners from config.yaml beside it; see .dagu/README.md.
# Dependencies and Chrome only; the code comes from the checkout at run time.

# Full image for its compiler: peewee ships as an sdist only.
FROM python:3.12-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /uvx /bin/
WORKDIR /app

# pyproject declares `readme = "README.md"`.
COPY pyproject.toml uv.lock README.md ./

# Build the venv at its final path so console-script shebangs stay valid.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
RUN uv sync --frozen --no-dev

FROM python:3.12-slim-bookworm
WORKDIR /workspace

COPY --from=builder /opt/venv /opt/venv

# The project is virtual in uv.lock, so it is imported from the checkout.
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/workspace \
    PYTHONUNBUFFERED=1

# Chrome for sgx_api.get_auth(); the .deb so apt resolves its libraries.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && curl -fsSL -o /tmp/chrome.deb \
        https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm -rf /tmp/chrome.deb /var/lib/apt/lists/*

RUN google-chrome --version

# Compared against the checkout's by preflight.sh.
COPY uv.lock /opt/uv.lock
