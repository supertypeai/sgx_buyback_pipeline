from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from sgx_scraper.llm.caller import invoke_structured_vision_llm
from pathlib import Path
from sgx_scraper.llm.prompts import BoardPageNumber, PromptCollections

import base64
import os
import pymupdf

os.environ["TORCH_DEVICE"] = "cpu"


class MarkerPdfParser:
    def __init__(self) -> None:
        self.models = create_model_dict()

    def parse(
        self,
        pdf_path: str,
        page_numbers: list[int] | None = None,
    ) -> str:
        config = {
            "mode": "fast",
            "disable_ocr": True,
            "output_format": "markdown",
            "paginate_output": True,
        }

        if page_numbers is not None:
            config["page_range"] = page_numbers

        converter = PdfConverter(
            artifact_dict=self.models,
            config=config,
        )

        rendered_output = converter(pdf_path)

        return rendered_output.markdown


def encode_image(image_path: str) -> str:
    image_bytes = Path(image_path).read_bytes()
    return base64.b64encode(image_bytes).decode("utf-8")


def run_llm_vision():
    prompts = PromptCollections()

    result = invoke_structured_vision_llm(
        pydantic_output=BoardPageNumber,
        system_prompt=prompts.get_system_board_page_vision_prompt(),
        user_prompt=prompts.get_user_board_page_vision_prompt(),
        image_path="sgx_scraper/fetch_managements/data/Screenshot From 2026-08-27 12-11-38.png",
        log_name="Extract board director pages range",
    )

    return result


def map_report_pages_with_page_labels(
    pdf_path: str,
    report_page_start: int,
    report_page_end: int | None = None,
) -> str | None:
    pdf_document = pymupdf.open(pdf_path)

    try:
        page_labels = pdf_document.get_page_labels()
        physical_page_numbers = []
        report_page_numbers = [report_page_start]

        if report_page_end is not None:
            if report_page_end < report_page_start:
                return None

            report_page_numbers.append(report_page_end)

        for report_page_number in report_page_numbers:
            physical_page_number = None

            for label_index, label_rule in enumerate(page_labels):
                if (
                    label_rule.get("style") != "D"
                    or label_rule.get("prefix")
                ):
                    continue

                rule_start_page = label_rule["startpage"]
                next_rule_start_page = (
                    page_labels[label_index + 1]["startpage"]
                    if label_index + 1 < len(page_labels)
                    else pdf_document.page_count
                )
                candidate_page_index = (
                    rule_start_page
                    + report_page_number
                    - label_rule["firstpagenum"]
                )

                if rule_start_page <= candidate_page_index < next_rule_start_page:
                    physical_page_number = candidate_page_index + 1
                    break

            if physical_page_number is None:
                return None

            physical_page_numbers.append(physical_page_number)

        if report_page_end is None:
            return str(physical_page_numbers[0])

        return f"{physical_page_numbers[0]}-{physical_page_numbers[1]}"
    finally:
        pdf_document.close()



if __name__ == "__main__":
    parser = MarkerPdfParser()

    # board_text = parser.parse(
    #     "sgx_scraper/fetch_managements/data/pdf/5ig_annual_report.pdf",
    #     page_numbers=[1],
    # )

    # print(board_text)

    # result = run_llm_vision()
    # print(result)

    pdf = "sgx_scraper/fetch_managements/data/pdf/5ig_annual_report.pdf"
    pdf_document = pymupdf.open(pdf)
    rules = pdf_document.get_page_labels()
    print(rules)
    print(map_report_pages_with_page_labels(pdf, 8, 9))

    
