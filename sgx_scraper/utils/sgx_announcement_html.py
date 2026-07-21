from bs4 import BeautifulSoup
from bs4.element import Tag

import logging


LOGGER = logging.getLogger(__name__)


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
