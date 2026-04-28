# DuckDB Parquet Data Schema

## Scope

This document defines the frontend analytics data contract for:
- Gold Parquet files
- DuckDB-WASM runtime queries in the browser

It replaces the previous Firestore-centric schema documentation for interactive analytics.

## Runtime architecture summary

- Gold files are served as static assets.
- Frontend loads those files with fetch.
- Files are registered in DuckDB-WASM as in-memory file buffers.
- SQL queries are executed client-side by parquet-data.service.

## Gold file set

Configured in environment parquetFiles:
- _conditions.parquet
- _h3_reference.parquet
- {condition}.parquet

Where {condition}.parquet resolves from conditionParquetTemplate, for example 840539006.parquet.

## Main table schemas

DuckDB reads each file with read_parquet(alias). The service supports compatible column aliases.

### 1) _conditions.parquet

Purpose:
- Condition list and display metadata
- Aggregate counts and filter metadata arrays

Columns used by frontend:
- condition_code or condition (string)
- condition_display or display_name (string)
- patient_count or case_count (number)
- generated_at (string ISO-8601, optional)
- total_unique (number, optional)
- age_groups_json (JSON string array, optional)
- genders_json (JSON string array, optional)

### 2) _h3_reference.parquet

Purpose:
- H3 -> lat/lon lookup for map rendering

Columns used by frontend:
- h3 or h3_id (string)
- latitude or lat (number)
- longitude or lon or lng (number)
- generated_at (string ISO-8601, optional)
- total_unique (number, optional)

### 3) {condition}.parquet

Purpose:
- Condition-specific fact table for map aggregates and timeline playback

Required columns (at least one alias in each set must exist):
- date_key or date
- h3
- case_count or casecount

Optional columns:
- age_group or agegroup
- gender
- condition_code or condition

Notes:
- If condition_code or condition exists, service applies equality filter by selected condition.
- If age_group and gender exist, service applies IN filters for demographic selections.

## Query contract used by parquet-data.service

### A) Metadata load

Reads:
- _conditions.parquet
- _h3_reference.parquet

Builds:
- selectable conditions
- age group and gender options
- H3 coordinate lookup
- overview counts and timestamps

### B) Aggregated hotspot query

Input filters:
- conditionCode
- startDate
- endDate
- ageGroups[]
- genders[]

Output fields:
- h3
- case_count sum per h3
- latest_date max per h3
- days_with_cases distinct date count per h3

### C) Daily series query

Input filters:
- same as aggregated query

Output fields:
- date_key
- h3
- case_count sum per date_key and h3

### D) Latest date query

Output field:
- MAX(date_key or date) as latest_date

## Data quality expectations

- Parquet payload must start with PAR1 magic bytes.
- Date values should be normalized to YYYY-MM-DD for stable filtering.
- case_count should be numeric and non-negative.
- H3 ids should match lookup records in _h3_reference.parquet.

## Hosting and source configuration

Frontend can load Gold Parquet from:
- local assets baseUrl
- CDN baseUrl

DuckDB bundle loading is configurable by environment:
- local bundle files under assets/duckdb
- optional CDN fallback bundle strategy

## Related implementation

- frontend/src/app/services/parquet-data.service.ts
- frontend/src/environments/environment.ts
- frontend/src/environments/environment.prod.ts