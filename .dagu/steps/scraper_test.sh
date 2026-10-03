#!/usr/bin/env bash
# Builds the CLI call from the inputs in workflows/scraper_test.yaml.
set -euo pipefail

if [[ "$SCRAPER_TYPE" == "buybacks" ]]; then
    args=(scraper_buybacks)
else
    args=(scraper_filings)
fi

if [[ -n "$PERIOD_START" ]]; then
    args+=(--period-start "$PERIOD_START")
fi

if [[ -n "$PERIOD_END" ]]; then
    args+=(--period-end "$PERIOD_END")
fi

if [[ -n "$PAGE_SIZE" ]]; then
    args+=(--page-size "$PAGE_SIZE")
fi

if [[ "$IS_PUSH_DB" == "true" ]]; then
    args+=(--is-push-db)
else
    args+=(--no-is-push-db)
fi

if [[ "$IS_PROXY" == "true" ]]; then
    args+=(--is-proxy)
else
    args+=(--no-is-proxy)
fi

printf 'Running command: python -m sgx_scraper.main_cli'
printf ' %q' "${args[@]}"
printf '\n'

exec python -m sgx_scraper.main_cli "${args[@]}"
