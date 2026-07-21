import requests


def fetch_compact_rates() -> dict:
    url = "https://raw.githubusercontent.com/supertypeai/sectors_sg_my_data_updater/main/compact_rates.json"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


