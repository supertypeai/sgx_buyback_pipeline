from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests


class HttpClient:
    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.8,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, url: str, **kwargs):
        timeout = kwargs.pop("timeout", self.timeout)
        return self.session.get(url, timeout=timeout, **kwargs)


HTTPCLIENT = HttpClient()
