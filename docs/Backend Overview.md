# Backend Architecture Overview

## What it does

Our backend is a staged healthcare data pipeline:
1. Pull FHIR data incrementally from https://r4.smarthealthit.org.
2. Store raw data in Bronze.
3. Normalize and dedupe into Silver.
4. Aggregate analytics outputs in Gold.
5. Publish Gold to Firestore for the frontend.

## Data flow (simple view)

1. Fetcher -> Bronze
- Reads last cursor from MongoDB.
- Calls https://r4.smarthealthit.org FHIR endpoints with `_lastUpdated=ge<cursor>` and pagination.
- Writes timestamped parquet snapshots per resource type.

2. Bronze -> Silver
- Parses raw FHIR JSON into typed columns.
- Dedupes by entity keys (patient_id, encounter_id, condition_id, etc.).
- Writes timestamped silver snapshots.

3. Silver -> Gold
- Reads Silver (usually by run timestamp).
- Aggregates condition counts by date and H3 geospatial cell.
- Writes partitioned gold files optimized for fast map/time queries.

4. Gold -> Firestore
- Pushes pre-aggregated results for fast UI reads.

## Why this scales

- Incremental pull: only new/updated records are fetched each run.
- Snapshot isolation: timestamped Bronze/Silver outputs make retries safer and debugging easier.
- Stage separation: fetch, transform, aggregate, and publish can be tuned independently.
- Query-ready Gold: heavy computation is done ahead of time, so frontend reads are fast.
- Metadata tracking: MongoDB stores cursor, run state, and job history for reliability.

## Implementation note

For the scope of this project, we moved from Hudi-based storage to basic Parquet + Spark processing.
This reduced operational complexity and avoided Hudi-specific dependency/catalog issues,
while keeping the pipeline simpler to run and debug.

## Validation data push we used

To validate ingestion end-to-end, we used a custom script:
- `local_testing/push_synthea_covid19_r4.py`

It pushes COVID-focused Synthea records 
from: https://synthetichealth.github.io/synthea-sample-data/downloads/10k_synthea_covid19_csv.zip
to: https://r4.smarthealthit.org

This lets us confirm that data published to the FHIR source is then pulled by our fetcher and appears downstream in Bronze/Silver/Gold.
