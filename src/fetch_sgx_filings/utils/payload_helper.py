from bs4 import BeautifulSoup 


def extract_section_data(soup: BeautifulSoup, section_title: str) -> dict[str, str | list[str]]:
    section_data = {}
    h2 = soup.find('h2', class_='announcement-group-header', string=section_title)
    if not h2: 
        return section_data
    
    section_div = h2.find_next_sibling('div', class_='announcement-group')
    if not section_div:
        return section_data
    
    dt_tags = section_div.find_all('dt')
    for dt in dt_tags:
        dd = dt.find_next_sibling('dd')
        if dd and not dd.find('table'):
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            if key:
                section_data[key] = value
                
    attachment_links = section_div.find_all('a', class_='announcement-attachment')
    if attachment_links:
        base_url = "https://links.sgx.com"
        urls = [f"{base_url}{link.get('href')}" for link in attachment_links if link.get('href')]
        if urls:
            section_data['attachments'] = urls

    return section_data