import unittest
from io import BytesIO
from unittest.mock import patch
from tempfile import TemporaryDirectory
from datetime import datetime

import fitz

from sgx_scraper.fetch_managements.management import (
    enrich_announcements,
    get_board_page_spec,
    management_parser,
)
from sgx_scraper.fetch_managements.utils.pdf_helper import (
    map_report_pages_to_pdf,
    parse_pages,
)
from sgx_scraper.utils.json_helper import open_json, write_json


def create_pdf(pages: list[list[tuple[float, float, str]]]) -> bytes:
    document = fitz.open()

    for page_entries in pages:
        page = document.new_page()

        for x_position, y_position, text in page_entries:
            page.insert_text((x_position, y_position), text)

    pdf_bytes = document.tobytes()
    document.close()
    return pdf_bytes


class TestMarkerPageParsing(unittest.TestCase):
    def test_marker_parser_receives_bytes_and_zero_based_page_numbers(self) -> None:
        with patch(
            "sgx_scraper.fetch_managements.utils.pdf_helper.MarkerPdfParser",
        ) as marker_parser_class:
            marker_parser = marker_parser_class.return_value
            marker_parser.parse.return_value = "marker output"

            result = parse_pages(
                pdf_bytes=b"pdf bytes",
                page_spec="1-2, 4",
                is_liteparse=False,
            )

        self.assertEqual(result, "marker output")

        parse_call = marker_parser.parse.call_args
        marker_input = parse_call.kwargs["pdf_path"]

        self.assertIsInstance(marker_input, BytesIO)
        self.assertEqual(marker_input.getvalue(), b"pdf bytes")
        self.assertEqual(parse_call.kwargs["page_numbers"], [0, 1, 3])


class TestReportPageMapping(unittest.TestCase):
    def test_footer_label_overrides_centered_contents_number(self) -> None:
        pdf_bytes = create_pdf(
            [
                [(291, 570, "147"), (291, 600, "151")],
                [(280, 770, "147")],
                [(280, 770, "151")],
            ]
        )

        pdf_page_spec = map_report_pages_to_pdf(
            pdf_bytes=pdf_bytes,
            report_page_start=147,
            report_page_end=151,
        )

        self.assertEqual(pdf_page_spec, "2-3")

    def test_centered_labels_are_used_without_header_or_footer_labels(self) -> None:
        pdf_bytes = create_pdf(
            [
                [(291, 570, "7")],
                [(291, 570, "8")],
            ]
        )

        pdf_page_spec = map_report_pages_to_pdf(
            pdf_bytes=pdf_bytes,
            report_page_start=7,
            report_page_end=8,
        )

        self.assertEqual(pdf_page_spec, "1-2")


class TestFallbackPageSelection(unittest.TestCase):
    def test_incomplete_fallback_range_returns_none_without_ocr(self) -> None:
        with (
            patch(
                "sgx_scraper.fetch_managements.management.get_contents_page_spec",
                return_value="1",
            ),
            patch(
                "sgx_scraper.fetch_managements.management.get_range_pages",
                return_value={"page_start": 67, "page_end": None},
            ),
            patch(
                "sgx_scraper.fetch_managements.management.find_board_pdf_pages_from_ocr",
            ) as find_board_pdf_pages_from_ocr,
        ):
            page_spec = get_board_page_spec(
                pdf_bytes=b"",
                table_of_content_page=1,
                models=[],
                is_fallback=True,
            )

        self.assertIsNone(page_spec)
        find_board_pdf_pages_from_ocr.assert_not_called()

    def test_missing_page_spec_returns_none_without_splitting(self) -> None:
        with (
            patch(
                "sgx_scraper.fetch_managements.management.parse_pages",
                return_value="contents",
            ),
            patch(
                "sgx_scraper.fetch_managements.management.get_contents_page",
                return_value=1,
            ),
            patch(
                "sgx_scraper.fetch_managements.management.get_board_page_spec",
                return_value=None,
            ),
        ):
            result = management_parser(
                pdf_bytes=b"",
                company_name="Example Company",
                models=[],
            )

        self.assertIsNone(result)


class TestAppointmentEnrichment(unittest.TestCase):
    def test_enriches_incomplete_start_date_and_missing_age(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            lookup_path = f"{temporary_directory}/lookup.json"
            management_path = f"{temporary_directory}/management.json"
            output_path = f"{temporary_directory}/enriched.json"

            write_json(
                lookup_path,
                {
                    "C41": [
                        {
                            "id": "not-a-director",
                            "title": "Appointment of Chief Financial Officer",
                            "submission_date": "20100101",
                        },
                        {
                            "id": "director-appointment",
                            "title": "Appointment of Executive Director",
                            "submission_date": "20100403",
                        },
                    ],
                },
            )
            write_json(
                management_path,
                [
                    {
                        "C41": {
                            "payload": [
                                {
                                    "name": "Dr Lim Jit Ming Raymond",
                                    "age": None,
                                    "start_date": "1992",
                                },
                                {
                                    "name": "Already Complete",
                                    "age": 60,
                                    "start_date": "15 April 2010",
                                },
                            ],
                        },
                    },
                ],
            )

            with patch(
                "sgx_scraper.fetch_managements.management.get_appointment",
                return_value={
                    "name": "Lim Jit Ming Raymond",
                    "age": "40",
                    "start_date": "2010-04-03",
                },
            ) as get_appointment:
                enriched_records = enrich_announcements(
                    lookup_ann_path=lookup_path,
                    management_path=management_path,
                    output_path=output_path,
                )

            enriched_payload = enriched_records[0]["C41"]["payload"]

            self.assertEqual(enriched_payload[0]["start_date"], "2010-04-03")
            self.assertEqual(
                enriched_payload[0]["age"],
                40 + (datetime.now().year - 2010),
            )
            self.assertEqual(
                enriched_payload[1]["start_date"],
                "15 April 2010",
            )
            get_appointment.assert_called_once()
            self.assertEqual(open_json(output_path), enriched_records)

            with patch(
                "sgx_scraper.fetch_managements.management.get_appointment",
            ) as get_appointment:
                enrich_announcements(
                    lookup_ann_path=lookup_path,
                    management_path=output_path,
                    output_path=output_path,
                )

            get_appointment.assert_not_called()


if __name__ == "__main__":
    unittest.main()
