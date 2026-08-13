from sgx_scraper.utils.http_client import HTTPCLIENT
from .base_parser import BaseFormParser
from .form_1 import Form1Parser
from .form_6 import Form6Parser
from .form_3.part_ii import Form3PartII
from .form_3.part_iii_iv import Form3PartIIIandIV

import re
import logging
import fitz


LOGGER = logging.getLogger(__name__)


class RouterFormParser:
    """
    Pick the right parser for an SGX filing from the document text.

    Form type comes from the notification-form title, within Form 3, the
    presence of the 'Part III' section heading (multiple substantial holders)
    selects Part III+IV, otherwise Part II.
    """

    @staticmethod
    def _select_parser_class(text: str) -> type[BaseFormParser] | None:
        if re.search(r'NOTIFICATION\s+FORM\s+FOR\s+DIRECTOR', text, re.IGNORECASE):
            return Form1Parser

        if re.search(r'NOTIFICATION\s+FORM\s+FOR\s+TRUSTEE', text, re.IGNORECASE):
            return Form6Parser

        if re.search(r'NOTIFICATION\s+FORM\s+FOR\s+SUBSTANTIAL', text, re.IGNORECASE):
            # 'Part III' also appears in the explanatory notes of every Form 3,
            # so match the actual section heading 'Part III - Substantial ...'
            if re.search(r'Part\s+III\s*[-–]\s*Substantial', text, re.IGNORECASE):
                return Form3PartIIIandIV

            return Form3PartII

        return None

    @staticmethod
    def get_parser(pdf_url: str) -> BaseFormParser | None:
        response = HTTPCLIENT.get(pdf_url)
        response.raise_for_status()
        pdf_bytes = response.content

        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = '\n'.join(page.get_text() for page in doc)

        parser_class = RouterFormParser._select_parser_class(text)

        if parser_class is None:
            return None

        LOGGER.info("[RouterFormParser] -> %s", parser_class.__name__)

        parser = parser_class(pdf_url)
        # reuse the bytes already downloaded for detection
        parser.pdf_bytes = pdf_bytes

        return parser

