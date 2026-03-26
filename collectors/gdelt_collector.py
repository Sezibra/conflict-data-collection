"""
GDELT BigQuery Collector Module

Reusable functions for collecting conflict event data from the GDELT
project via Google BigQuery. Handles authentication, query construction,
and data quality assessment.

Usage:
    from collectors.gdelt_collector import collect_gdelt_events, assess_quality

    df = collect_gdelt_events(
        credentials_path='credentials/bigquery-key.json',
        country_code='ET',
        date_start='20201101',
        date_end='20221130'
    )
    quality_report = assess_quality(df)
"""

import os
import pandas as pd
from google.cloud import bigquery


# CAMEO conflict event root codes
CONFLICT_ROOT_CODES = ['14', '17', '18', '19', '20']

CAMEO_LABELS = {
    '14': 'Protest',
    '17': 'Coerce',
    '18': 'Assault',
    '19': 'Fight',
    '20': 'Mass Violence'
}


def get_client(credentials_path=None, project=None):
    """
    Initialize a BigQuery client.

    Parameters:
        credentials_path (str): Path to service account JSON key
        project (str): Google Cloud project ID (auto-detected if None)

    Returns:
        bigquery.Client
    """
    if credentials_path:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    return bigquery.Client(project=project)


def build_conflict_query(country_code, date_start, date_end,
                         root_codes=None):
    """
    Build a SQL query for GDELT conflict events.

    Parameters:
        country_code (str): Two-letter FIPS country code (e.g., 'ET')
        date_start (str): Start date as YYYYMMDD
        date_end (str): End date as YYYYMMDD
        root_codes (list): CAMEO root codes to include

    Returns:
        str: SQL query string
    """
    if root_codes is None:
        root_codes = CONFLICT_ROOT_CODES

    code_filter = ' OR '.join(
        [f"EventRootCode = '{c}'" for c in root_codes]
    )

    return f"""
    SELECT
        SQLDATE, MonthYear,
        EventCode, EventBaseCode, EventRootCode,
        QuadClass, GoldsteinScale,
        NumMentions, NumSources, NumArticles, AvgTone,
        Actor1Name, Actor1CountryCode, Actor1Type1Code,
        Actor2Name, Actor2CountryCode, Actor2Type1Code,
        ActionGeo_FullName, ActionGeo_CountryCode,
        ActionGeo_Lat, ActionGeo_Long,
        SOURCEURL
    FROM `gdelt-bq.gdeltv2.events`
    WHERE ActionGeo_CountryCode = '{country_code}'
    AND CAST(SQLDATE AS STRING) BETWEEN '{date_start}' AND '{date_end}'
    AND ({code_filter})
    ORDER BY SQLDATE ASC
    """


def collect_gdelt_events(credentials_path, country_code, date_start,
                         date_end, root_codes=None, verbose=True):
    """
    Collect GDELT conflict events via BigQuery.

    Parameters:
        credentials_path (str): Path to service account key
        country_code (str): FIPS country code
        date_start (str): Start date (YYYYMMDD)
        date_end (str): End date (YYYYMMDD)
        root_codes (list): CAMEO root codes
        verbose (bool): Print progress

    Returns:
        pd.DataFrame: Raw event data
    """
    client = get_client(credentials_path)
    query = build_conflict_query(country_code, date_start, date_end,
                                  root_codes)

    if verbose:
        print(f'Querying GDELT: {country_code}, {date_start}-{date_end}')
        print('Executing query...')

    df = client.query(query).to_dataframe()

    if verbose:
        print(f'Retrieved {len(df):,} events')

    return df


def clean_gdelt_events(df, verbose=True):
    """
    Clean raw GDELT event data.

    Steps:
        1. Remove URL/code/date duplicates
        2. Parse dates
        3. Add category labels
        4. Flag low-confidence events
        5. Flag missing geolocation

    Parameters:
        df (pd.DataFrame): Raw GDELT data
        verbose (bool): Print progress

    Returns:
        pd.DataFrame: Cleaned data
    """
    result = df.copy()

    # Deduplicate
    before = len(result)
    result = result.drop_duplicates(
        subset=['SOURCEURL', 'EventCode', 'SQLDATE'], keep='first'
    )
    if verbose:
        removed = before - len(result)
        print(f'Removed {removed:,} duplicates')

    # Parse dates
    result['date'] = pd.to_datetime(
        result['SQLDATE'].astype(str), format='%Y%m%d', errors='coerce'
    )
    result['year_month'] = result['date'].dt.to_period('M')

    # Labels
    result['event_category'] = result['EventRootCode'].map(
        {k: f'{v} ({k}x)' for k, v in CAMEO_LABELS.items()}
    )

    # Quality flags
    result['low_confidence'] = result['NumMentions'] == 1
    result['has_geolocation'] = result['ActionGeo_Lat'].notna()

    if verbose:
        print(f'Cleaned rows: {len(result):,}')

    return result


def assess_quality(df):
    """
    Generate a data quality report for GDELT data.

    Parameters:
        df (pd.DataFrame): GDELT event data

    Returns:
        dict: Quality metrics
    """
    total = len(df)
    return {
        'total_events': total,
        'unique_urls': df['SOURCEURL'].nunique(),
        'missing_geo': df['ActionGeo_Lat'].isnull().sum(),
        'missing_geo_pct': df['ActionGeo_Lat'].isnull().mean() * 100,
        'single_mention': (df['NumMentions'] == 1).sum(),
        'single_mention_pct': (df['NumMentions'] == 1).mean() * 100,
        'missing_actor1': df['Actor1Name'].isnull().sum(),
        'missing_actor2': df['Actor2Name'].isnull().sum(),
        'avg_goldstein': df['GoldsteinScale'].mean(),
        'avg_tone': df['AvgTone'].mean()
    }
