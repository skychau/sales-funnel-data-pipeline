# Sales Funnel Data Pipeline - currently MVP version

## Overview

This project builds an end-to-end analytics pipeline for tracking sales funnel metrics.

## Stack

- Python (data ingestion)
- BigQuery (data warehouse)
- dbt (transformations)

## Data Flow

Python → BigQuery (raw_leads) → dbt (stg_leads → fct_funnel)

## Models

- stg_leads: cleans and deduplicates lead data
- fct_funnel: aggregates leads by source

## How to run

### 1. Load data

python pipelines/load_data.py

### 2. Run dbt

cd dbt_project
dbt run
dbt test

## Future Improvements

- Add opportunity data for conversion rate
- Add Airflow orchestration
- Add CI/CD with GitHub Actions
