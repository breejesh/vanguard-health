# Firebase Data Schema

## Scope
This document describes the Firebase data model used by this project.

## Firestore 

### 1) gold_overview
Document pattern:
- Collection: `gold_overview`
- Document id: `current`
- Cardinality: single document used as global metadata/metrics

Fields:
| Field | Type | Required | Source | Notes |
|---|---|---|---|---|
| generated_at | string (ISO-8601) | no | pusher | Generation time from `_conditions.json` (or fallback now). |
| h3_resolution | number | no | pusher | H3 resolution from `_h3_reference.json`. |
| total_conditions | number | no | pusher + prune script | Number of condition codes in scope. |
| total_h3_cells | number | no | pusher | Count of H3 cells in lookup. |
| condition_codes | array<string> | no | pusher + prune script | Available condition code list. |
| total_cases | number | no | pusher | Sum of case_count over all pushed daily docs. |
| total_cases_overall | number | no | pusher | Alias of total_cases for UI compatibility. |
| last_update_time | string (ISO-8601) | no | pusher | Last run completion timestamp. |
| updated_at | Firestore server timestamp | no | pusher + prune script | Write timestamp in Firestore. |

Example:
```json
{
  "generated_at": "2026-04-05T02:15:17.330152+00:00",
  "h3_resolution": 4,
  "total_conditions": 2,
  "total_h3_cells": 489,
  "condition_codes": ["840539006", "840544004"],
  "total_cases": 124937,
  "total_cases_overall": 124937,
  "last_update_time": "2026-04-05T02:20:03.889421+00:00",
  "updated_at": "<server_timestamp>"
}
```

### 2) gold_conditions
Document pattern:
- Collection: `gold_conditions`
- Document id: `<condition_code>` (string)
- Cardinality: one document per condition code

Fields:
| Field | Type | Required | Source | Notes |
|---|---|---|---|---|
| condition_code | string | yes | pusher | Usually matches document id. |
| display_name | string | yes | pusher | Human-readable condition label for UI. |
| patient_count | number | yes | pusher | Aggregated count from Gold metadata. |
| updated_at | Firestore server timestamp | no | pusher | Write timestamp in Firestore. |

Example:
```json
{
  "condition_code": "840539006",
  "display_name": "COVID-19",
  "patient_count": 52133,
  "updated_at": "<server_timestamp>"
}
```

### 3) gold_h3_cells
Document pattern:
- Collection: `gold_h3_cells`
- Document id: `<h3>`
- Cardinality: one document per H3 cell id

Fields:
| Field | Type | Required | Source | Notes |
|---|---|---|---|---|
| h3 | string | yes | pusher | H3 cell id (string). |
| latitude | number | no | pusher | Cell center latitude. |
| longitude | number | no | pusher | Cell center longitude. |
| h3_resolution | number | no | pusher | Resolution of this cell set. |
| updated_at | Firestore server timestamp | no | pusher | Write timestamp in Firestore. |

Example:
```json
{
  "h3": "842a107ffffffff",
  "latitude": 33.7488,
  "longitude": -84.3877,
  "h3_resolution": 4,
  "updated_at": "<server_timestamp>"
}
```

### 4) gold_daily_cells
Document pattern:
- Collection: `gold_daily_cells`
- Document id: `<condition_code>_<date_key>_<h3>`
- Cardinality: many documents (condition x day x h3)

Fields:
| Field | Type | Required | Source | Notes |
|---|---|---|---|---|
| condition_code | string | yes | pusher | Condition code filter key. |
| date_key | string (YYYY-MM-DD) | yes | pusher | Day bucket used for date filtering. |
| h3 | string | yes | pusher | H3 cell id. |
| case_count | number | yes | pusher | Cases for this condition/day/cell. |
| updated_at | Firestore server timestamp | no | pusher | Write timestamp in Firestore. |

Example:
```json
{
  "condition_code": "840539006",
  "date_key": "2026-04-05",
  "h3": "842a107ffffffff",
  "case_count": 19,
  "updated_at": "<server_timestamp>"
}
```

## Frontend Read Patterns

Current UI query patterns:
1. Load global stats:
   - `gold_overview/current`
2. Load selectable conditions:
   - `gold_conditions` ordered by `display_name`
3. Load map/time data for selected condition and rolling date window:
   - `gold_daily_cells`
   - `where(condition_code == <selected>)`
   - `where(date_key >= <start>)`
   - `where(date_key <= <end>)`

## Firestore Index Guidance

To support the rolling window map query efficiently, keep indexing aligned with the access pattern:
1. Single-field indexes for `condition_code` and `date_key` (usually automatic).
2. Composite index recommendation:
   - Collection: `gold_daily_cells`
   - Fields: `condition_code` ASC, `date_key` ASC

If Firestore returns an index creation error URL for the query, create the suggested index in the target project.

## Write Semantics and Idempotency

1. Most writes use merge semantics for metadata docs (`gold_overview/current`, `gold_conditions/*`).
2. `gold_h3_cells` and `gold_daily_cells` are upserted by deterministic document ids.
3. Re-running push for the same Gold payload updates existing documents instead of duplicating records.
4. Batched writes are committed in chunks below Firestore batch limits.

