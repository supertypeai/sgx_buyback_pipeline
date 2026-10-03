#!/usr/bin/env bash
# usage: preflight.sh [chrome]
set -euo pipefail

fail() { echo "preflight: $*" >&2; exit 1; }

cmp -s /opt/uv.lock uv.lock || fail "uv.lock changed since the image was built; redeploy buyback-runners in runners (.dagu/README.md)"

# Also fails on an empty SUPABASE_URL or SUPABASE_KEY.
python -c 'import sgx_scraper.main_cli' || fail "cannot import sgx_scraper (traceback above)"

chrome=""
if [ "${1:-}" = chrome ]; then
    # Same root/Docker flags as sgx_api.get_wire_driver().
    google-chrome --headless=new --no-sandbox --disable-dev-shm-usage --disable-gpu --dump-dom about:blank >/dev/null 2>&1 \
        || fail "google-chrome does not start in this container"
    chrome=", $(google-chrome --version)"
fi

echo "preflight ok: $(git rev-parse --short HEAD)${chrome}"
