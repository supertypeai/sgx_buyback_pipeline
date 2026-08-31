from bs4 import BeautifulSoup
from bs4.element import Tag
from urllib.parse import unquote, urljoin, urlparse

from sgx_scraper.utils.http_client import HTTPCLIENT

import logging
import re


LOGGER = logging.getLogger(__name__)


def normalize_document_name(document_name: str) -> str:
    return re.sub(r'[^a-z0-9]+', ' ', document_name.casefold()).strip()


def get_company_initialism(company_name: str) -> str:
    company_words = re.findall(r'[A-Za-z0-9]+', company_name)
    initialism_parts = []

    for company_word in company_words:
        if company_word.isupper() and 1 < len(company_word) <= 4:
            initialism_parts.append(company_word.casefold())
        else:
            initialism_parts.append(company_word[0].casefold())

    return ''.join(initialism_parts)


def is_annual_report_attachment(
    attachment_url: str,
    attachment_name: str
) -> bool:
    url_filename = unquote(urlparse(attachment_url).path.rsplit('/', 1)[-1])
    normalized_name = normalize_document_name(
        f'{attachment_name} {url_filename}'
    )

    if (
        any(
            excluded_term in normalized_name
            for excluded_term in (
                'appendix',
                'appendices',
                'addendum',
                'circular',
                'corrigendum',
                'corrigenda',
                'errata',
                'erratum',
                'sustainability report',
                'letter to shareholders',
                'agm',
                'booklet',
                'request form',
            )
        ) or
        re.search(r'\bchairman(?:s| s)? message\b', normalized_name) is not None or
        re.search(r'\b(?:page|pg)\s*\d+\b', normalized_name) is not None or
        re.search(r'\bpart\s*(?:0*[2-9]|[1-9]\d+)\b', normalized_name) is not None
    ):
        return False

    return (
        'annual report' in normalized_name
        or 'annualreport' in normalized_name
        or re.search(r'\bar(?:\s*\d{2,4})?\b', normalized_name) is not None
        or re.search(r'ar\s*20\d{2}\b', normalized_name) is not None
    )


def attachment_matches_company(
    attachment_url: str,
    attachment_name: str,
    company_name: str | None,
) -> bool:
    if not company_name:
        return False

    url_filename = unquote(urlparse(attachment_url).path.rsplit('/', 1)[-1])
    normalized_company_name = normalize_document_name(company_name)
    normalized_attachment_name = normalize_document_name(
        f'{attachment_name} {url_filename}'
    )

    if normalized_company_name in normalized_attachment_name:
        return True

    company_initialism = get_company_initialism(company_name)

    if len(company_initialism) < 4:
        return False

    return re.search(
        rf'\b{re.escape(company_initialism)}\b',
        normalized_attachment_name,
    ) is not None


def is_annual_report_announcement(soup: BeautifulSoup) -> bool:
    for label_tag in soup.find_all('dt'):
        normalized_label = normalize_document_name(
            label_tag.get_text(' ', strip=True)
        )

        if normalized_label != 'report type':
            continue

        value_tag = label_tag.find_next_sibling('dd')

        if not value_tag:
            return False

        normalized_report_type = normalize_document_name(
            value_tag.get_text(' ', strip=True)
        )

        return normalized_report_type == 'annual report'

    return False


def is_agm_named_annual_report_attachment(
    attachment_url: str,
    attachment_name: str,
) -> bool:
    url_filename = unquote(urlparse(attachment_url).path.rsplit('/', 1)[-1])
    normalized_name = normalize_document_name(
        f'{attachment_name} {url_filename}'
    )

    if 'agm' not in normalized_name:
        return False

    if not (
        'annual report' in normalized_name
        or 'annualreport' in normalized_name
        or re.search(r'\bar(?:\s*\d{2,4})?\b', normalized_name) is not None
        or re.search(r'ar\s*20\d{2}\b', normalized_name) is not None
    ):
        return False

    if any(
        excluded_term in normalized_name
        for excluded_term in (
            'appendix',
            'appendices',
            'addendum',
            'circular',
            'corrigendum',
            'corrigenda',
            'errata',
            'erratum',
            'sustainability report',
            'letter to shareholders',
            'booklet',
            'request form',
        )
    ):
        return False

    if re.search(r'\bchairman(?:s| s)? message\b', normalized_name):
        return False

    if re.search(r'\b(?:page|pg)\s*\d+\b', normalized_name):
        return False

    return re.search(
        r'\bpart\s*(?:0*[2-9]|[1-9]\d+)\b',
        normalized_name,
    ) is None


def get_language_variant_suffix(attachment_name: str) -> tuple[str, str] | None:
    attachment_stem = attachment_name.rsplit('.', 1)[0].casefold()
    language_match = re.fullmatch(r'([ec])[_\-\s]*(.+)', attachment_stem)

    if not language_match:
        return None

    return language_match.groups()


def select_english_language_pair(
    annual_report_attachments: list[tuple[str, str]],
) -> str | None:
    if len(annual_report_attachments) != 2:
        return None

    first_url, first_name = annual_report_attachments[0]
    second_url, second_name = annual_report_attachments[1]
    first_variant = get_language_variant_suffix(first_name)
    second_variant = get_language_variant_suffix(second_name)

    if not first_variant or not second_variant:
        return None

    first_language, first_suffix = first_variant
    second_language, second_suffix = second_variant

    if (
        first_suffix != second_suffix
        or {first_language, second_language} != {'e', 'c'}
    ):
        return None

    if first_language == 'e':
        return first_url

    return second_url


def is_safe_sole_annual_report_attachment(
    attachment_url: str,
    attachment_name: str,
) -> bool:
    url_filename = unquote(urlparse(attachment_url).path.rsplit('/', 1)[-1])
    normalized_name = normalize_document_name(
        f'{attachment_name} {url_filename}'
    )

    if any(
        excluded_term in normalized_name
        for excluded_term in (
            'appendix',
            'appendices',
            'addendum',
            'circular',
            'corrigendum',
            'corrigenda',
            'errata',
            'erratum',
            'sustainability report',
            'letter to shareholders',
            'agm',
            'booklet',
            'request form',
        )
    ):
        return False

    if re.search(r'\bchairman(?:s| s)? message\b', normalized_name):
        return False

    if re.search(r'\b(?:page|pg)\s*\d+\b', normalized_name):
        return False

    return re.search(
        r'\bpart\s*(?:0*[2-9]|[1-9]\d+)\b',
        normalized_name,
    ) is None


def is_direct_document_url(document_url: str) -> bool:
    return 'FileOpen' in document_url or '.ashx' in document_url


def fetch_announcement_soup(announcement_url: str) -> BeautifulSoup:
    response = HTTPCLIENT.get(announcement_url)
    response.raise_for_status()
    return BeautifulSoup(response.text, 'html.parser')


def extract_attachment_urls(
    soup: BeautifulSoup,
) -> list[tuple[str, str]]:
    attachment_urls = []

    for link in soup.find_all('a', class_='announcement-attachment'):
        attachment_path = link.get('href')

        if not attachment_path:
            continue

        attachment_url = urljoin('https://links.sgx.com', attachment_path)
        attachment_name = link.get_text(' ', strip=True)
        attachment_urls.append((attachment_url, attachment_name))

    return attachment_urls


def extract_issuer_name(soup: BeautifulSoup) -> str | None:
    issuer_labels = {'issuer manager', 'issuer company name'}

    for label_tag in soup.find_all('dt'):
        normalized_label = re.sub(
            r'[^a-z0-9]+',
            ' ',
            label_tag.get_text(' ', strip=True).casefold(),
        ).strip()

        if normalized_label not in issuer_labels:
            continue

        value_tag = label_tag.find_next_sibling('dd')

        if value_tag:
            issuer_name = value_tag.get_text(' ', strip=True)

            if issuer_name:
                return issuer_name

    return None


def resolve_document_url(
    announcement_url: str,
) -> str | None:
    if is_direct_document_url(announcement_url):
        return announcement_url

    soup = fetch_announcement_soup(announcement_url)
    
    attachment_urls = [
        attachment_url
        for attachment_url, _ in extract_attachment_urls(soup)
    ]

    if not attachment_urls:
        LOGGER.info(
            '[sgx_announcement] no attachment found for %s',
            announcement_url
        )

        return None

    return attachment_urls[-1]


def resolve_annual_report(
    announcement_url: str,
    company_name: str | None = None,
) -> tuple[str | None, str | None]:
    if is_direct_document_url(announcement_url):
        return announcement_url, company_name

    soup = fetch_announcement_soup(announcement_url)
    resolved_company_name = company_name or extract_issuer_name(soup)

    attachment_urls = extract_attachment_urls(soup)
    
    annual_report_attachments = [
        (attachment_url, attachment_name)
        for attachment_url, attachment_name in attachment_urls
        if is_annual_report_attachment(attachment_url, attachment_name)
    ]

    if (
        not annual_report_attachments
        and is_annual_report_announcement(soup)
    ):
        annual_report_attachments = [
            (attachment_url, attachment_name)
            for attachment_url, attachment_name in attachment_urls
            if is_agm_named_annual_report_attachment(
                attachment_url,
                attachment_name,
            )
        ]

    if len(annual_report_attachments) == 1:
        return annual_report_attachments[0][0], resolved_company_name

    if len(annual_report_attachments) > 1:
        company_matched_attachments = [
            attachment_url
            for attachment_url, attachment_name in annual_report_attachments
            if attachment_matches_company(
                attachment_url,
                attachment_name,
                resolved_company_name,
            )
        ]

        if len(company_matched_attachments) == 1:
            return company_matched_attachments[0], resolved_company_name

        english_language_pair_url = select_english_language_pair(
            annual_report_attachments
        )

        if english_language_pair_url:
            return english_language_pair_url, resolved_company_name

        LOGGER.info(
            '[sgx_announcement] ambiguous annual report attachments for %s',
            announcement_url,
        )
        
        return None, resolved_company_name

    generic_attachment_urls = [
        attachment_url
        for attachment_url, attachment_name in attachment_urls
        if re.fullmatch(r'attachment\s+\d+\.pdf', attachment_name, re.IGNORECASE)
    ]

    if len(attachment_urls) == 1 and len(generic_attachment_urls) == 1:
        LOGGER.info(
            '[sgx_announcement] using sole generic attachment for %s',
            announcement_url,
        )
        return generic_attachment_urls[0], resolved_company_name

    if (
        len(attachment_urls) == 1
        and is_annual_report_announcement(soup)
    ):
        attachment_url, attachment_name = attachment_urls[0]

        if is_safe_sole_annual_report_attachment(
            attachment_url,
            attachment_name,
        ):
            LOGGER.info(
                '[sgx_announcement] using sole annual report attachment for %s',
                announcement_url,
            )
            return attachment_url, resolved_company_name

    LOGGER.info(
        '[sgx_announcement] no annual report attachment found for %s',
        announcement_url,
    )

    return None, resolved_company_name


def extract_table_data(table_element: Tag) -> dict[str, str | list[str]]:
    try:
        table_data = {}
        rows = table_element.find_all('tr')

        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 2:
                continue

            key = cells[0].get_text(strip=True)
            values = []

            for cell in cells[1:]:
                text = cell.get_text(strip=True)

                if text:
                    values.append(text)

            if not key or not values:
                continue

            if len(values) == 1:
                table_data[key] = values[0]

            else:
                table_data[key] = values

        return table_data

    except Exception as error:
        LOGGER.error(f"[extract_table_data] Failed to extract table data: {error}")
        return None


def extract_section_data(soup: BeautifulSoup, section_title: str) -> dict[str, str | list[str]]:
    section_data = {}

    try:
        h2 = soup.find(
            'h2',
            class_='announcement-group-header',
            string=lambda text: bool(text) and text.strip() == section_title
        )

        if not h2:
            return section_data

        section_div = h2.find_next_sibling('div', class_='announcement-group')

        if not section_div:
            return section_data

        # Extract simple key-value pairs where the <dd> does not contain a table
        dt_tags = section_div.find_all('dt')

        for dt in dt_tags:
            dd = dt.find_next_sibling('dd')

            if dd and not dd.find('table'):
                key = dt.get_text(strip=True)
                value = dd.get_text(strip=True)

                if key:
                    section_data[key] = value

        # Find all tables within the section
        all_tables = section_div.find_all('table')

        for table in all_tables:
            table_data = extract_table_data(table)
            section_data.update(table_data)

        for key in list(section_data.keys()):
            if 'total consideration' in key.lower().strip():
                section_data['Total Consideration'] = section_data.pop(key)

        return section_data

    except Exception as error:
        LOGGER.error(f'[extract_section_data] Error extracting section {section_title}: {error}')
        return None
