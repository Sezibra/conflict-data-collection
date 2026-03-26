"""
Web Scraper Module for Conflict Data Collection

Reusable functions for scraping humanitarian report listings
from web pages. Implements polite scraping practices: delays
between requests, descriptive User-Agent, robots.txt checking.

Usage:
    from collectors.scraper import scrape_reliefweb_listings

    reports = scrape_reliefweb_listings(
        search_query='Ethiopia+Tigray',
        max_pages=15,
        delay=2.0
    )
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re


BASE_URL = 'https://reliefweb.int'

DEFAULT_HEADERS = {
    'User-Agent': (
        'ConflictDataCollection/1.0 '
        '(Academic research project; CSS portfolio; Python requests)'
    )
}


def check_robots_txt(base_url=BASE_URL, headers=None):
    """
    Fetch and display a site's robots.txt.

    Parameters:
        base_url (str): Root URL of the site
        headers (dict): Request headers

    Returns:
        str: Contents of robots.txt, or empty string on failure
    """
    if headers is None:
        headers = DEFAULT_HEADERS

    try:
        r = requests.get(f'{base_url}/robots.txt', headers=headers, timeout=15)
        if r.status_code == 200:
            return r.text
    except requests.exceptions.RequestException:
        pass
    return ''


def parse_reliefweb_page(html_content):
    """
    Parse a ReliefWeb updates page and extract report listings.

    Parameters:
        html_content (str): Raw HTML

    Returns:
        list: List of dicts with report metadata
    """
    soup = BeautifulSoup(html_content, 'lxml')
    reports = []

    # Strategy 1: article elements
    articles = soup.find_all('article')
    if articles:
        for article in articles:
            report = {}

            heading = article.find(['h2', 'h3', 'h4'])
            if heading:
                link = heading.find('a')
                if link:
                    report['title'] = link.get_text(strip=True)
                    href = link.get('href', '')
                    report['url'] = href if href.startswith('http') else BASE_URL + href
                else:
                    report['title'] = heading.get_text(strip=True)
                    report['url'] = ''

            time_tag = article.find('time')
            if time_tag:
                report['date'] = time_tag.get('datetime', time_tag.get_text(strip=True))
            else:
                report['date'] = ''

            source_elem = article.find(class_=re.compile(r'source|org|author', re.I))
            report['source'] = source_elem.get_text(strip=True) if source_elem else ''

            snippet_elem = article.find(
                class_=re.compile(r'snippet|summary|desc|body|excerpt', re.I)
            )
            report['snippet'] = snippet_elem.get_text(strip=True)[:500] if snippet_elem else ''

            if report.get('title'):
                reports.append(report)

        return reports

    # Strategy 2: links to /report/ paths
    report_links = soup.find_all('a', href=re.compile(r'/report/'))
    seen_urls = set()

    for link in report_links:
        href = link.get('href', '')
        full_url = href if href.startswith('http') else BASE_URL + href

        if full_url in seen_urls or len(link.get_text(strip=True)) < 10:
            continue
        seen_urls.add(full_url)

        report = {
            'title': link.get_text(strip=True),
            'url': full_url,
            'date': '',
            'source': '',
            'snippet': ''
        }

        parent = link.find_parent(['article', 'div', 'li'])
        if parent:
            time_tag = parent.find('time')
            if time_tag:
                report['date'] = time_tag.get('datetime', time_tag.get_text(strip=True))

        reports.append(report)

    return reports


def scrape_reliefweb_listings(search_query, max_pages=20, delay=2.0,
                               headers=None, verbose=True):
    """
    Scrape multiple pages of ReliefWeb search results.

    Parameters:
        search_query (str): URL-encoded search query
        max_pages (int): Maximum pages to scrape
        delay (float): Seconds between requests
        headers (dict): Request headers
        verbose (bool): Print progress

    Returns:
        list: All scraped report listings
    """
    if headers is None:
        headers = DEFAULT_HEADERS

    all_reports = []

    if verbose:
        print(f'Scraping ReliefWeb for: "{search_query}"')

    for page_num in range(max_pages):
        offset = page_num * 20
        url = f'{BASE_URL}/updates?search={search_query}&view=reports&offset={offset}'

        try:
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code != 200:
                if verbose:
                    print(f'  Page {page_num + 1}: HTTP {response.status_code}, stopping.')
                break

            page_reports = parse_reliefweb_page(response.text)

            if not page_reports:
                if verbose:
                    print(f'  Page {page_num + 1}: No reports found, stopping.')
                break

            all_reports.extend(page_reports)
            if verbose:
                print(f'  Page {page_num + 1}: {len(page_reports)} reports (total: {len(all_reports)})')

            if page_num < max_pages - 1:
                time.sleep(delay)

        except requests.exceptions.RequestException as e:
            if verbose:
                print(f'  Page {page_num + 1}: Error - {str(e)[:100]}')
            break

    if verbose:
        print(f'Scraping complete: {len(all_reports)} reports')

    return all_reports


def scraped_to_dataframe(reports):
    """
    Convert scraped report listings to a pandas DataFrame.

    Parameters:
        reports (list): List of report dicts

    Returns:
        pd.DataFrame
    """
    df = pd.DataFrame(reports)
    if 'date' in df.columns:
        df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
        df['year_month'] = df['date_parsed'].dt.to_period('M')
    df = df.drop_duplicates(subset=['url'], keep='first')
    return df
