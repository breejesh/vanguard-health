# Fetcher API Specs

## Purpose
This document lists the outbound HTTP API calls used by the backend fetch/push flow.

## Base endpoint
- Pull source (FHIR R4): https://r4.smarthealthit.org
- In code, base URL is configurable with `SYNTHEA_API_URL` and normalized to avoid double `/fhir` suffixes.

## Common request headers (pull)
- `Accept: application/fhir+json`
- Optional: `Authorization: Bearer <token>` when API key/token is configured

## 1) Incremental resource pull (primary fetch call)
- Method: `GET`
- URL pattern: `/<ResourceType>`
- Query params:
  - `_lastUpdated=ge<ISO8601 timestamp>`
  - `_count=100`
  - `_format=json`
- Resource types pulled:
  - `Patient`
  - `Encounter`
  - `Observation`
  - `Condition`
  - `Medication`
  - `MedicationRequest`
- Timeout: 30 seconds per request
- Pagination:
  - Follows `link[].relation == "next"` until no next link exists
- Used by:
  - `src/ingestion/fetcher_job.py` via `FHIRFetcher.fetch_incremental()`

Example:
`GET https://r4.smarthealthit.org/Patient?_lastUpdated=ge2026-04-04T00:00:00Z&_count=100&_format=json`

## 2) Bulk pull (local testing / backfill utility path)
- Method: `GET`
- URL pattern: `/<ResourceType>`
- Query params:
  - `_count=100`
  - `_format=json`
- Timeout: 30 seconds per request
- Pagination:
  - Same `next`-link traversal as incremental mode
- Used by:
  - `FHIRFetcher.fetch_bulk()` (mostly local/testing usage)

Example:
`GET https://r4.smarthealthit.org/Condition?_count=100&_format=json`

## 3) Single patient fetch
- Method: `GET`
- URL pattern: `/Patient/{id}`
- Timeout: 30 seconds
- Used by:
  - `FHIRFetcher.fetch_patient()`

Example:
`GET https://r4.smarthealthit.org/Patient/12345`

## 4) Connectivity check
- Method: `GET`
- URL pattern: `/Patient`
- Query params:
  - `_count=1`
- Timeout: 10 seconds
- Used by:
  - `FHIRFetcher.test_connection()`

Example:
`GET https://r4.smarthealthit.org/Patient?_count=1`

## 5) Validation push API (custom COVID script)
- Script: `local_testing/push_synthea_covid19_r4.py`
- Method: `POST`
- URL: base FHIR endpoint root (for transaction bundle processing)
- Headers:
  - `Content-Type: application/fhir+json`
  - `Accept: application/fhir+json`
  - Optional `Authorization: Bearer <token>`
- Timeout: configurable (`--timeout`, default 30 seconds)
- Payload:
  - FHIR `Bundle` with `type: transaction`
  - Entries include:
    - `PUT Patient?identifier=<system>|<value>` (conditional upsert)
    - `PUT Encounter?identifier=<system>|<value>` (conditional upsert)
    - `POST Condition` (create)
- Purpose:
  - Seed COVID-focused test data into https://r4.smarthealthit.org and verify backend ingestion picks it up.

Example:
`POST https://r4.smarthealthit.org`

## Error/behavior notes
- HTTP errors propagate through `response.raise_for_status()` in fetch paths.
- Pagination continues only while a valid `next` link is present.
- Fetch cursor advances only after Bronze write succeeds, reducing risk of data loss on partial failures.
