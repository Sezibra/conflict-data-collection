"""
ReliefWeb API Collector Module

Reusable functions for collecting humanitarian reports from the
ReliefWeb API (api.reliefweb.int). Handles request construction,
pagination, error handling, and response parsing.

Usage:
    from collectors.reliefweb_collector import collect_reports, parse_reports_to_dataframe
    
    raw_reports, total = collect_reports(
        country='Ethiopia',
        date_start='2020-11-01',
        date_end='2022-11-30'
    )
    df = parse_reports_to_dataframe(raw_reports)
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime


API_URL = 'https://api.reliefweb.int/v1/reports'

DEFAULT_FIELDS = [
    'id', 'title', 'date.original', 'source',
    'body', 'url', 'format', 'theme', 'status'
]


def build_request_payload(country, date_start, date_end, fields=None,
                          limit=1000, offset=0):
    """
    Build the JSON payload for a ReliefWeb API request.

    Parameters:
        country (str): Country name to filter by
        date_start (str): Start date in YYYY-MM-DD format
        date_end (str): End date in YYYY-MM-DD format
        fields (list): Fields to retrieve (uses defaults if None)
        limit (int): Results per page (max 1000)
        offset (int): Number of results to skip

    Returns:
        dict: JSON-serializable payload
    """
    if fields is None:
        fields = DEFAULT_FIELDS

    return {
        'appname': 'conflict-data-collection-portfolio',
        'filter': {
            'operator': 'AND',
            'conditions': [
                {'field': 'country.name', 'value': country},
                {'field': 'date.original', 'value': {'from': date_start, 'to': date_end}}
            ]
        },
        'fields': {'include': fields},
        'limit': limit,
        'offset': offset,
        'sort': ['date.original:asc']
    }


def make_api_request(payload, max_retries=3, timeout=30):
    """
    Make a single POST request to the ReliefWeb API with retry logic.

    Parameters:
        payload (dict): JSON payload
        max_retries (int): Maximum retry attempts
        timeout (int): Request timeout in seconds

    Returns:
        dict: Parsed JSON response

    Raises:
        Exception: If all retries fail
    """
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, json=payload, timeout=timeout)

            if response.status_code == 200:
                return response.json()

            if response.status_code in [429, 500, 502, 503, 504]:
                wait_time = 2 ** (attempt + 1)
                print(f'  Status {response.status_code}, retrying in {wait_time}s '
                      f'(attempt {attempt + 1}/{max_retries})')
                time.sleep(wait_time)
                continue

            response.raise_for_status()

        except requests.exceptions.Timeout:
            wait_time = 2 ** (attempt + 1)
            print(f'  Timeout, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})')
            time.sleep(wait_time)

        except requests.exceptions.ConnectionError:
            wait_time = 2 ** (attempt + 1)
            print(f'  Connection error, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})')
            time.sleep(wait_time)

    raise Exception(f'API request failed after {max_retries} attempts')


def collect_reports(country, date_start, date_end, page_size=1000,
                    delay=1.0, verbose=True):
    """
    Collect all ReliefWeb reports matching the given filters.

    Parameters:
        country (str): Country name
        date_start (str): Start date (YYYY-MM-DD)
        date_end (str): End date (YYYY-MM-DD)
        page_size (int): Results per page (max 1000)
        delay (float): Seconds to wait between requests
        verbose (bool): Print progress messages

    Returns:
        tuple: (list of raw report dicts, int total count)
    """
    all_reports = []
    offset = 0
    total_count = None

    if verbose:
        print(f'Collecting ReliefWeb reports: {country}, {date_start} to {date_end}')

    while True:
        payload = build_request_payload(country, date_start, date_end,
                                        limit=page_size, offset=offset)
        response_data = make_api_request(payload)

        if total_count is None:
            total_count = response_data['totalCount']
            if verbose:
                print(f'Total reports to collect: {total_count}')

        batch = response_data.get('data', [])
        if not batch:
            break

        all_reports.extend(batch)
        if verbose:
            print(f'  Collected {len(all_reports)} / {total_count}')

        offset += page_size
        if offset >= total_count:
            break

        time.sleep(delay)

    if verbose:
        print(f'Collection complete: {len(all_reports)} reports')

    return all_reports, total_count


def parse_single_report(report):
    """
    Parse a single raw API report into a flat dictionary.

    Parameters:
        report (dict): Raw report from the API

    Returns:
        dict: Flat dictionary with parsed fields
    """
    fields = report.get('fields', {})

    sources = fields.get('source', [])
    source_names = [s.get('name', 'Unknown') for s in sources]
    primary_source = source_names[0] if source_names else 'Unknown'

    formats = fields.get('format', [])
    format_name = formats[0].get('name', 'Unknown') if formats else 'Unknown'

    themes = fields.get('theme', [])
    theme_names = [t.get('name', '') for t in themes]

    date_info = fields.get('date', {})
    date_str = date_info.get('original', None) if isinstance(date_info, dict) else None

    body = fields.get('body', '')

    return {
        'id': report.get('id', ''),
        'title': fields.get('title', ''),
        'date': date_str,
        'primary_source': primary_source,
        'all_sources': '; '.join(source_names),
        'format_type': format_name,
        'themes': '; '.join(theme_names),
        'body_text': body,
        'url': fields.get('url', ''),
        'body_length': len(body) if body else 0
    }


def parse_reports_to_dataframe(raw_reports):
    """
    Parse a list of raw API reports into a pandas DataFrame.

    Parameters:
        raw_reports (list): List of raw report dicts

    Returns:
        pd.DataFrame: Parsed and typed DataFrame
    """
    records = [parse_single_report(r) for r in raw_reports]
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
    df['year_month'] = df['date'].dt.to_period('M')
    return df
