from liteparse import LiteParse
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict

from io import BytesIO

import logging
import re
import pymupdf
import os 


LOGGER = logging.getLogger(__name__)

os.environ["TORCH_DEVICE"] = "cpu"
os.environ["FAST_DETECTOR_DEVICE"] = "cpu"


class MarkerPdfParser:
    def __init__(self) -> None:
        self.models = create_model_dict()

    def parse(
        self,
        pdf_path: str | BytesIO,
        page_numbers: list[int] | None = None,
    ) -> str:
        config = {
            "mode": "fast",
            "disable_ocr": True,
            "output_format": "markdown",
            "paginate_output": True,
            "disable_links": True
        }

        if page_numbers is not None:
            config["page_range"] = page_numbers

        converter = PdfConverter(
            artifact_dict=self.models,
            config=config,
        )

        rendered_output = converter(pdf_path)

        return rendered_output.markdown


def parse_pages(
    pdf_bytes: bytes, 
    page_spec: str = "1-5, 10", 
    is_liteparse: bool = True 
) -> str:
    if is_liteparse:
        complexity_report = LiteParse(
            target_pages=page_spec,
            quiet=True,
        ).is_complex(pdf_bytes)

        needs_ocr = any(
            page_report.needs_ocr 
            for page_report in complexity_report
        )

        parser = LiteParse(
            target_pages=page_spec,
            ocr_enabled=needs_ocr,
            output_format="markdown",
            quiet=True,
        )

        parse_result = parser.parse(pdf_bytes)

        return parse_result.text 
    
    page_numbers = []

    for page_item in page_spec.split(","):
        normalized_page_item = page_item.strip()

        if "-" in normalized_page_item:
            page_start_text, page_end_text = normalized_page_item.split("-", 1)
            page_start = int(page_start_text)
            page_end = int(page_end_text)
            page_numbers.extend(range(page_start - 1, page_end))
            continue

        page_numbers.append(int(normalized_page_item) - 1)

    marker_parser = MarkerPdfParser()
    page_result = marker_parser.parse(
        pdf_path=BytesIO(pdf_bytes),
        page_numbers=page_numbers,
    )

    return page_result


def parse_native_pages(pdf_bytes: bytes, page_spec: str) -> str:
    page_numbers = []

    for page_item in page_spec.split(","):
        normalized_page_item = page_item.strip()

        if "-" in normalized_page_item:
            page_start_text, page_end_text = normalized_page_item.split("-", 1)

            if not page_start_text.isdigit() or not page_end_text.isdigit():
                return ""

            page_start = int(page_start_text)
            page_end = int(page_end_text)

            if page_end < page_start:
                return ""

            page_numbers.extend(range(page_start, page_end + 1))
            continue

        if not normalized_page_item.isdigit():
            return ""

        page_numbers.append(int(normalized_page_item))

    pdf_document = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    page_texts = []

    try:
        for page_number in page_numbers:
            if page_number < 1 or page_number > pdf_document.page_count:
                continue

            page_text = pdf_document[page_number - 1].get_text("text", sort=True)

            if page_text.strip():
                page_texts.append(
                    f"--- PDF page {page_number} ---\n{page_text.strip()}"
                )
    finally:
        pdf_document.close()

    return "\n\n".join(page_texts)


def map_report_pages_with_page_labels(
    pdf_bytes: bytes,
    report_page_start: int,
    report_page_end: int | None = None,
) -> str | None:
    if report_page_end is None:
        report_page_end = report_page_start

    if report_page_end < report_page_start:
        return None

    pdf_document = pymupdf.open(
        stream=pdf_bytes,
        filetype="pdf",
    )

    try:
        start_candidates = pdf_document.get_page_numbers(
            str(report_page_start)
        )
        end_candidates = pdf_document.get_page_numbers(
            str(report_page_end)
        )

        if not start_candidates or not end_candidates:
            return None

        expected_distance = (
            report_page_end - report_page_start
        )

        matching_ranges = [
            (start_index, end_index)
            for start_index in start_candidates
            for end_index in end_candidates
            if (
                end_index >= start_index
                and end_index - start_index == expected_distance
            )
        ]

        if len(matching_ranges) != 1:
            return None

        start_index, end_index = matching_ranges[0]

        return f"{start_index}-{end_index}"

    finally:
        pdf_document.close()


def map_report_pages_to_pdf(
    pdf_bytes: bytes,
    report_page_start: int | None,
    report_page_end: int | None,
) -> str | None:
    if report_page_start is None:
        return None

    if report_page_end is not None and report_page_end < report_page_start:
        LOGGER.warning(
            "Invalid printed report page range: %s-%s",
            report_page_start,
            report_page_end,
        )
        return None

    pdf_document = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    target_report_pages = {report_page_start}

    if report_page_end is not None:
        target_report_pages.add(report_page_end)

    report_page_candidates = {
        report_page_number: []
        for report_page_number in target_report_pages
    }
    report_page_to_label_priority = {}

    try:
        for physical_page_index in range(pdf_document.page_count):
            page = pdf_document[physical_page_index]

            if all(
                report_page_to_label_priority.get(report_page_number) == 2
                for report_page_number in target_report_pages
            ):
                break

            header_end = page.rect.height * 0.15
            footer_start = page.rect.height * 0.88

            for block in page.get_text("blocks"):
                is_header_block = block[3] <= header_end
                is_footer_block = block[1] >= footer_start
                block_center = (block[0] + block[2]) / 2

                is_centered_page_number_block = (
                    block[1] >= page.rect.height * 0.65
                    and page.rect.width * 0.4 <= block_center
                    <= page.rect.width * 0.6
                    and len(block[4].splitlines()) == 1
                )

                is_header_or_footer_block = (
                    is_header_block or is_footer_block
                )

                if (
                    not is_header_or_footer_block
                    and not is_centered_page_number_block
                ):
                    continue

                label_priority = 2 if is_header_or_footer_block else 1

                for line in block[4].splitlines():
                    normalized_line = line.strip()

                    page_number_match = re.fullmatch(
                        r"0*([0-9]{1,3})",
                        normalized_line,
                    )

                    if not page_number_match:
                        page_number_match = re.fullmatch(
                            r"-\s*0*([0-9]{1,3})\s*-",
                            normalized_line,
                        )

                    if (
                        not page_number_match
                        and (is_header_block or is_footer_block)
                    ):
                        compact_line = re.sub(r"\s+", "", normalized_line)
                        page_number_match = re.fullmatch(
                            r"0*([0-9]{1,3})[.|]?",
                            compact_line,
                        )

                    if (
                        not page_number_match
                        and (is_header_block or is_footer_block)
                    ):
                        page_number_match = re.match(
                            r"^0*([0-9]{1,3})(?![0-9])(?:\s|[.|]|$)",
                            normalized_line,
                        )

                    if (
                        not page_number_match
                        and (is_header_block or is_footer_block)
                    ):
                        page_number_match = re.match(
                            r"^[IVXLCDM]+\s+0*([0-9]{1,3})(?![0-9])",
                            normalized_line,
                        )

                    page_number_matches = []

                    if page_number_match:
                        page_number_matches.append(page_number_match)

                    if (
                        is_header_block or is_footer_block
                    ) and ("|" in normalized_line or "/" in normalized_line):
                        page_number_matches.extend(
                            re.finditer(
                                r"(?<![0-9])0*([0-9]{1,3})(?![0-9])",
                                normalized_line,
                            )
                        )

                    if not page_number_matches:
                        continue

                    for page_number_match in page_number_matches:
                        report_page_number = int(page_number_match.group(1))

                        if report_page_number not in target_report_pages:
                            continue

                        report_page_candidates[report_page_number].append(
                            (physical_page_index + 1, label_priority)
                        )
                        report_page_to_label_priority[report_page_number] = max(
                            report_page_to_label_priority.get(
                                report_page_number,
                                0,
                            ),
                            label_priority,
                        )
    finally:
        pdf_document.close()

    preferred_report_page_candidates = {}

    for report_page_number, candidates in report_page_candidates.items():
        if not candidates:
            continue

        highest_label_priority = max(
            candidate_label_priority
            for candidate_pdf_page, candidate_label_priority in candidates
        )
        preferred_report_page_candidates[report_page_number] = [
            candidate_pdf_page
            for candidate_pdf_page, candidate_label_priority in candidates
            if candidate_label_priority == highest_label_priority
        ]

    report_page_to_pdf_page = {}
    start_page_candidates = preferred_report_page_candidates.get(
        report_page_start,
        [],
    )
    end_page_candidates = preferred_report_page_candidates.get(
        report_page_end,
        [],
    )

    if (
        report_page_end is not None
        and start_page_candidates
        and end_page_candidates
    ):
        printed_page_distance = report_page_end - report_page_start
        best_pdf_page_start = None
        best_pdf_page_end = None
        best_distance_difference = None

        for candidate_pdf_page_start in start_page_candidates:
            for candidate_pdf_page_end in end_page_candidates:
                if candidate_pdf_page_end < candidate_pdf_page_start:
                    continue

                pdf_page_distance = (
                    candidate_pdf_page_end - candidate_pdf_page_start
                )
                distance_difference = abs(
                    pdf_page_distance - printed_page_distance
                )

                if (
                    best_distance_difference is None
                    or distance_difference < best_distance_difference
                ):
                    best_pdf_page_start = candidate_pdf_page_start
                    best_pdf_page_end = candidate_pdf_page_end
                    best_distance_difference = distance_difference

        if best_pdf_page_start is not None:
            report_page_to_pdf_page[report_page_start] = best_pdf_page_start
            report_page_to_pdf_page[report_page_end] = best_pdf_page_end

    for report_page_number, candidates in preferred_report_page_candidates.items():
        if report_page_number not in report_page_to_pdf_page:
            report_page_to_pdf_page[report_page_number] = candidates[0]

    pdf_page_start = report_page_to_pdf_page.get(report_page_start)
    pdf_page_end = report_page_to_pdf_page.get(report_page_end)

    if pdf_page_start is None:
        LOGGER.warning(
            "Could not map printed report page %s to a PDF page",
            report_page_start,
        )
        return None

    if report_page_end is None:
        return str(pdf_page_start)

    if pdf_page_end is None:
        LOGGER.warning(
            "Could not map printed report page %s to a PDF page",
            report_page_end,
        )
        return None

    if pdf_page_end < pdf_page_start:
        LOGGER.warning(
            "Invalid PDF page range mapped from printed pages %s-%s: %s-%s",
            report_page_start,
            report_page_end,
            pdf_page_start,
            pdf_page_end,
        )
        return None

    LOGGER.info(
        "Mapped printed report pages %s-%s to PDF pages %s-%s",
        report_page_start,
        report_page_end,
        pdf_page_start,
        pdf_page_end,
    )

    return f"{pdf_page_start}-{pdf_page_end}"


def resolve_report_pages_to_pdf(
    pdf_bytes: bytes,
    report_page_start: int | None,
    report_page_end: int | None,
) -> str | None:
    if report_page_start is None:
        return None

    page_label_range = map_report_pages_with_page_labels(
        pdf_bytes=pdf_bytes,
        report_page_start=report_page_start,
        report_page_end=report_page_end,
    )

    if page_label_range is None:
        LOGGER.info("[MAPPER PDF PAGES FALLBACK] fires")
        return map_report_pages_to_pdf(
            pdf_bytes=pdf_bytes,
            report_page_start=report_page_start,
            report_page_end=report_page_end,
        )

    page_label_start_text, page_label_end_text = page_label_range.split(
        "-",
        1,
    )
    pdf_page_start = int(page_label_start_text) + 1
    pdf_page_end = int(page_label_end_text) + 1

    LOGGER.info(
        "Mapped printed report pages %s-%s using PDF page labels to PDF "
        "pages %s-%s",
        report_page_start,
        report_page_end,
        pdf_page_start,
        pdf_page_end,
    )

    if report_page_end is None or report_page_start is None:
        return None 

    return f"{pdf_page_start}-{pdf_page_end}"
