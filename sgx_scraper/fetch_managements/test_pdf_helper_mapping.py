import unittest

import fitz

from sgx_scraper.fetch_managements.utils.pdf_helper import (
    map_report_pages_to_pdf,
)


def create_pdf(pages: list[list[tuple[float, float, str]]]) -> bytes:
    document = fitz.open()

    for page_entries in pages:
        page = document.new_page()

        for x_position, y_position, text in page_entries:
            page.insert_text((x_position, y_position), text)

    pdf_bytes = document.tobytes()
    document.close()
    return pdf_bytes


class TestReportPageMapping(unittest.TestCase):
    def test_same_priority_candidates_use_printed_range_distance(self) -> None:
        pdf_bytes = create_pdf(
            [
                [(57, 715, "36\nProfile of the Board of Directors")],
                [(42, 40, "36\nANNUAL REPORT 2025")],
                [(42, 40, "41\nANNUAL REPORT 2025")],
            ]
        )

        pdf_page_spec = map_report_pages_to_pdf(
            pdf_bytes=pdf_bytes,
            report_page_start=36,
            report_page_end=41,
        )

        self.assertEqual(pdf_page_spec, "2-3")


if __name__ == "__main__":
    unittest.main()
