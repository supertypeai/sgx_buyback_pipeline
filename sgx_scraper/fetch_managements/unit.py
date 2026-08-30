from pathlib import Path

from sgx_scraper.fetch_managements.management import (
    get_board_page_spec,
    get_contents_page,
)
from sgx_scraper.fetch_managements.utils.pdf_helper import parse_pages
from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.utils.sgx_announcement_html import resolve_annual_report
from sgx_scraper.utils.json_helper import open_json

import unittest


FAILED_RESULTS_PATH = Path(
    "sgx_scraper/fetch_managements/failed_result_parsed.json"
)
CLEANED_ANNOUNCEMENTS_PATH = Path(
    "sgx_scraper/fetch_managements/cleaned_announcements.json"
)


def get_test_symbols(
    failed_results: list[dict],
    selected_symbols: list[str] | None,
) -> list[str]:
    if selected_symbols:
        return selected_symbols

    return [record["symbol"] for record in failed_results]


def resolve_symbol_annual_report(
    announcements: list[dict],
) -> str | None:
    for announcement in announcements:
        annual_report_url, _ = resolve_annual_report(
            announcement_url=announcement["url"],
            company_name=(
                announcement.get("security_name")
                or announcement.get("issuer_name")
            ),
        )

        if annual_report_url:
            return annual_report_url

    return None


class TestAnnualReportSelection(unittest.TestCase):
    selected_symbols = None

    def test_1_requested_symbols(self) -> None:
        failed_results = open_json(FAILED_RESULTS_PATH)
        cleaned_announcements = open_json(CLEANED_ANNOUNCEMENTS_PATH)
        test_symbols = get_test_symbols(
            failed_results,
            self.selected_symbols,
        )

        for symbol in test_symbols:
            with self.subTest(symbol=symbol):
                announcements = cleaned_announcements.get(symbol)

                self.assertIsNotNone(
                    announcements,
                    f"{symbol} is not in cleaned_announcements.json",
                )

                annual_report_url = resolve_symbol_annual_report(announcements)
                print(f"{symbol}: {annual_report_url}")

    def test_2_page_range(self) -> None:
        failed_results = open_json(FAILED_RESULTS_PATH)
        cleaned_announcements = open_json(CLEANED_ANNOUNCEMENTS_PATH)

        test_symbols = get_test_symbols(
            failed_results,
            self.selected_symbols,
        )

        for symbol in test_symbols:
            with self.subTest(symbol=symbol):
                announcements = cleaned_announcements.get(symbol)

                self.assertIsNotNone(
                    announcements,
                    f"{symbol} is not in cleaned_announcements.json",
                )

                annual_report_url = resolve_symbol_annual_report(announcements)

                if annual_report_url is None:
                    print(f"{symbol}: no annual report URL")
                    continue

                response = HTTPCLIENT.get(annual_report_url)
                response.raise_for_status()
                pdf_bytes = response.content

                pdf_text = parse_pages(pdf_bytes)
                contents_page = get_contents_page(
                    pdf_texts=pdf_text,
                    pdf_bytes=pdf_bytes,
                )
                board_page_spec = get_board_page_spec(
                    pdf_bytes=pdf_bytes,
                    table_of_content_page=contents_page,
                    models=["deepsek-v4-flash","nvidia-nemotron-3-ultra"],
                )

                print(
                    f"{symbol}: contents={contents_page} "
                    f"board_pdf_pages={board_page_spec}"
                )

                self.assertIsNotNone(
                    board_page_spec,
                    f"{symbol}: could not resolve board PDF pages",
                )


if __name__ == "__main__":
    test_symbols = [
        "EMI",
        "H15",
        "5UF",
        "F03",
        "T6I",
        "TSH",
        "NIO",
        "J85",
        "AVP",

        # "F99"
    ]

    TestAnnualReportSelection.selected_symbols = test_symbols

    test_suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        TestAnnualReportSelection
    )

    test_result = unittest.TextTestRunner(verbosity=2).run(test_suite)

    if not test_result.wasSuccessful():
        raise SystemExit(1)
