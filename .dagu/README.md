# Dagu workflows

The SGX scrapers as [Dagu](https://dagu.sh) DAGs, deployed by
[runners](https://github.com/supertypeai/runners). Each deploys as
`sgx_buyback_pipeline--<file>`. Dagu caps DAG names below 40 characters, so
names longer than 17 characters are shortened from the GitHub ones.

| `.dagu/workflows/` | `.github/workflows/` | Schedule (UTC) |
| --- | --- | --- |
| `sgx_filings.yaml` | `sgx_filings_scraper.yaml` | `10 2 * * *` |
| `reit_transaction.yaml` | `sgx_reit_transaction_scraper.yaml` | `10 3 * * *` |
| `sgx_agm_scraper.yaml` | `sgx_agm_scraper.yaml` | `30 3 * * *` |
| `sgx_takeover.yaml` | `sgx_takeover_scraper.yaml` | `10 4 * * *` |
| `sgx_buyback.yaml` | `sgx_buyback_scraper.yaml` | `10 5 * * *` |
| `mgmt_tracking.yaml` | `management_tracking.yaml` | `10 6 * * *` |
| `shrholders_track.yaml` | `shareholders_tracking.yaml` | `0 7 * * *` |
| `upcoming_dividend.yaml` | `sgx_upcoming_dividend_scraper.yaml` | `0 18 * * 1-5` |
| `sync_screener.yaml` | `sync_screener_shareholders.yaml` | `0 8 1 * *` |
| `refresh_companies.yaml` | `refresh_sgx_companies.yaml` | `0 21 1 * *` |
| `scraper_test.yaml` | `scraper_test.yaml` | manual |

Schedules are plain cron in the host's timezone, which on the VPS is UTC, so they match GitHub.

Every scheduled workflow is in the `sgx-scraper-main` queue, so at most one
runs at a time. Each `timeout_sec` is about twice the longest GitHub run, so a
hung run frees the queue within hours rather than six.

`sgx_management` stays on GitHub Actions. It loads marker's torch models,
several GB on CPU, which the VPS cannot spare beside its other services.

## Runtime image

The scrapers need Chrome (for `sgx_api.get_auth()`) and torch (via
`marker-pdf`), so they run in a prebuilt image, `sgx-buyback-runners:latest`.
`.dagu/containers/buyback-runners/config.yaml` names it and the Dockerfile
beside it.

To build or rebuild it, open runners → Repositories → `sgx_buyback_pipeline` and
deploy `buyback-runners`. Runners builds the image on the host, starts the `buyback-runners`
container, and deletes the image it replaced. The build is a run of
`sgx_buyback_pipeline--buyback-runners`, so its log is on that DAG's page. The
Containers page shows the result.

The `buyback-runners` container only runs `sleep infinity`. It keeps the image
in use, so `docker image prune -a` cannot delete it between runs.

The image has no code, so only a `uv.lock` change needs a rebuild.
`preflight.sh` fails the run if the image is out of date.

The container runs as root, so `get_wire_driver()` starts Chrome with
`--no-sandbox` and `--disable-dev-shm-usage`.

## Host setup

1. Deploy `buyback-runners` to build the image, as above, and wait for its build to succeed.
2. Set these secrets at `/deploy` → the key icon on `sgx_buyback_pipeline`:

   ```
   SUPABASE_URL  SUPABASE_KEY  PROXY  GROQ_API_KEY  OPENROUTER_API_KEY
   AWS_ACCESS_KEY_ID  AWS_SECRET_ACCESS_KEY  AWS_REGION  SENDER_EMAIL  TO_EMAIL
   ```

3. Define the `sgx-scraper-main` queue in `$DAGU_HOME/config.yaml`, then
   `systemctl restart dagu`:

   ```yaml
   queues:
     enabled: true
     config:
       - name: sgx-scraper-main
         max_concurrency: 1
   ```

## Notes

- `scraper_test` takes its inputs from `container.env`. Dagu rejects
  undeclared params and runners owns `params:`, so edit the values and redeploy.
- The GitHub schedules are still on. Disable each one once its Dagu workflow is
  deployed, or both will run. Leave `sgx_management.yaml` on.
