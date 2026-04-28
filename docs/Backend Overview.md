# Backend Architecture Overview

## What it does

Vanguard Health uses a staged analytics pipeline and serves query-ready Parquet assets to the frontend.
The browser runs DuckDB-WASM to execute interactive filters directly on those files.

End-to-end stages:
1. Fetch incremental FHIR R4 data from https://r4.smarthealthit.org.
2. Store raw snapshots in Bronze.
3. Normalize and dedupe into Silver.
4. Aggregate Gold outputs for map and timeline analytics.
5. Serve Gold Parquet files as static assets for in-browser DuckDB queries.

## Data flow

1. Fetcher -> Bronze
- Reads incremental cursor/state from MongoDB.
- Calls FHIR endpoints with _lastUpdated cursor and pagination.
- Writes timestamped Parquet snapshots by resource type.

2. Bronze -> Silver
- Parses FHIR JSON into typed tabular records.
- Applies normalization and deduplication keys.
- Writes timestamped Silver Parquet snapshots.

3. Silver -> Gold
- Builds analytics-friendly aggregates by condition, date, H3 cell, age group, and gender.
- Produces per-condition Parquet files and supporting metadata Parquet files.

4. Gold -> Frontend runtime
- Gold files are hosted from local assets or CDN.
- Frontend downloads Parquet files and runs SQL with DuckDB-WASM.
- No Firestore reads are required for the interactive analytics path.

## Gold artifacts consumed by frontend

The frontend expects these files under the configured Gold base URL:
- _conditions.parquet
- _h3_reference.parquet
- {condition}.parquet (per condition code)

See DuckDB Parquet schema details in DuckDB Parquet Data Schema.md.

## Runtime query model

- Metadata load:
	- Reads _conditions.parquet and _h3_reference.parquet.
- Aggregate hotspot query:
	- Groups by H3 over a date window and demographic filters.
- Daily series query:
	- Groups by date_key and H3 for animation/timeline playback.
- Latest data date query:
	- MAX(date_key) or MAX(date) from the selected condition file.

All query execution is performed in-browser by DuckDB-WASM.

## Why this scales

- Incremental ingestion limits upstream API load.
- Stage isolation keeps fetch/transform/aggregate concerns independent.
- Pre-aggregated Gold files reduce frontend compute and network chatter.
- Columnar Parquet plus DuckDB vectorized execution keeps interactive filters fast.
- Static hosting simplifies deployment and avoids operational DB query infrastructure for the UI path.

## Validation path used in this project

For end-to-end validation we used:
- local_testing/push_synthea_covid19_r4.py

It pushes COVID-focused Synthea data to:
- https://r4.smarthealthit.org

This validates that published FHIR resources flow through Bronze, Silver, and Gold, then appear in frontend DuckDB queries.
