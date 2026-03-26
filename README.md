# Data Directory

This directory contains collected data for the Tigray conflict reporting analysis.

## Structure

- `raw/` — Data as originally retrieved from each source. Never modified after collection.
- `processed/` — Cleaned, filtered, and analysis-ready datasets.

## Data Sources

1. **ReliefWeb API** — UN OCHA humanitarian reports on Ethiopia (Nov 2020 – Nov 2022)
2. **GDELT via BigQuery** — Media event data for Ethiopia from global news sources
3. **Web scraping** — Humanitarian briefings from structured web sources
4. **UCDP GED v25.1** — Reference conflict event data from Project 1

## Reproduction

Run the notebooks in order (01 through 04) to regenerate all datasets.
Raw data files are not tracked in git. Run the collection pipelines to generate them.