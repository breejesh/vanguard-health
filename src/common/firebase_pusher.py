"""
Push Gold outputs to Google Firestore.

This is run as a standalone Kubernetes Job/Pod after Silver->Gold completes.
"""
import sys
import os
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import firebase_admin
from firebase_admin import credentials, firestore
import pyarrow.parquet as pq

from src.common.logger import setup_logger
from src.common.config import get_config

logger = setup_logger(__name__)
config = get_config()


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def _safe_doc_token(value: str) -> str:
    """Sanitize values used inside Firestore doc ids."""
    return str(value).replace("/", "_").strip()


def _read_parquet_rows(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    table = pq.read_table(str(path))
    return table.to_pylist()


def load_conditions_metadata(gold_dir: Path) -> Dict:
    """Load condition metadata from parquet first, then JSON fallback."""
    conditions_parquet = gold_dir / "_conditions.parquet"
    if conditions_parquet.exists():
        rows = _read_parquet_rows(conditions_parquet)
        condition_codes = []
        condition_display_map = {}
        condition_patient_counts = {}
        generated_at = None
        age_groups = []
        genders = []

        for row in rows:
            code_raw = row.get("condition_code")
            if code_raw is None:
                continue
            code = str(code_raw)
            if code not in condition_display_map:
                condition_codes.append(code)
            condition_display_map[code] = str(row.get("condition_display") or code)
            condition_patient_counts[code] = int(row.get("patient_count") or 0)

            if not generated_at and row.get("generated_at"):
                generated_at = str(row.get("generated_at"))
            if not age_groups and row.get("age_groups_json"):
                try:
                    age_groups = list(json.loads(row.get("age_groups_json")))
                except Exception:
                    age_groups = []
            if not genders and row.get("genders_json"):
                try:
                    genders = list(json.loads(row.get("genders_json")))
                except Exception:
                    genders = []

        return {
            "generated_at": generated_at,
            "conditions": condition_codes,
            "condition_display_map": condition_display_map,
            "condition_patient_counts": condition_patient_counts,
            "total_unique": len(condition_codes),
            "age_groups": age_groups,
            "genders": genders,
        }

    return load_json(gold_dir / "_conditions.json")


def load_h3_reference(gold_dir: Path) -> Dict:
    """Load H3 metadata from parquet first, then JSON fallback."""
    h3_parquet = gold_dir / "_h3_reference.parquet"
    if h3_parquet.exists():
        rows = _read_parquet_rows(h3_parquet)
        h3_cells = {}
        generated_at = None
        h3_resolution = int(os.getenv("H3_RESOLUTION", "4"))

        for row in rows:
            h3_raw = row.get("h3")
            if h3_raw is None:
                continue
            h3_id = str(h3_raw)
            h3_cells[h3_id] = {
                "latitude": float(row.get("latitude")) if row.get("latitude") is not None else None,
                "longitude": float(row.get("longitude")) if row.get("longitude") is not None else None,
            }
            if not generated_at and row.get("generated_at"):
                generated_at = str(row.get("generated_at"))

        return {
            "generated_at": generated_at,
            "h3_resolution": h3_resolution,
            "h3_cells": h3_cells,
            "total_unique": len(h3_cells),
        }

    return load_json(gold_dir / "_h3_reference.json")


def _resolve_credentials_path() -> Path:
    env_path = os.getenv("FIREBASE_CREDENTIALS_PATH")
    if env_path:
        return Path(env_path)

    explicit_filename = "vanguard-health-firebase-adminsdk-fbsvc-d3d5fbe79f.json"
    candidates = [
        Path(explicit_filename),
        Path("config") / explicit_filename,
        Path("/app") / explicit_filename,
        Path("/app/config") / explicit_filename,
        Path(config.FIREBASE_CREDENTIALS_PATH),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    return Path(config.FIREBASE_CREDENTIALS_PATH)


def init_firestore_client():
    if firebase_admin._apps:
        return firestore.client()

    credentials_env = os.getenv("FIREBASE_CREDENTIALS_JSON") or os.getenv("FIREBASE_CREDENTIALS")
    if credentials_env:
        raw_value = credentials_env.strip()

        # Support either inline JSON credentials or a path-like env value.
        if raw_value.startswith("{"):
            try:
                cred_info = json.loads(raw_value)
                cred = credentials.Certificate(cred_info)
                firebase_admin.initialize_app(cred, {'projectId': config.FIREBASE_PROJECT_ID})
                logger.info("Initialized Firestore using credentials JSON from environment")
                return firestore.client()
            except Exception as e:
                logger.warning(f"Failed parsing FIREBASE_CREDENTIALS as JSON: {e}")
        else:
            env_path_candidate = Path(raw_value)
            if env_path_candidate.exists() and env_path_candidate.is_file():
                cred = credentials.Certificate(str(env_path_candidate))
                firebase_admin.initialize_app(cred, {'projectId': config.FIREBASE_PROJECT_ID})
                logger.info(f"Initialized Firestore using credentials file from environment path: {env_path_candidate}")
                return firestore.client()
            logger.warning("FIREBASE_CREDENTIALS is set but is neither valid JSON nor an existing file path; falling back to configured credential paths")

    cred_path = _resolve_credentials_path()
    if not cred_path.exists():
        raise FileNotFoundError(f"Firebase credentials file not found: {cred_path}")

    cred = credentials.Certificate(str(cred_path))
    firebase_admin.initialize_app(cred, {'projectId': config.FIREBASE_PROJECT_ID})
    logger.info(f"Initialized Firestore using credentials file: {cred_path}")
    return firestore.client()


def _flush_batch(db_client, batch, pending_ops: int) -> Tuple[object, int]:
    if pending_ops > 0:
        batch.commit()
    return db_client.batch(), 0


def push_gold_to_firestore(db_client, gold_dir: Path) -> None:
    conditions_meta = load_conditions_metadata(gold_dir)
    h3_ref = load_h3_reference(gold_dir)

    condition_codes = conditions_meta.get("conditions", []) or []
    condition_display_map = conditions_meta.get("condition_display_map", {}) or {}
    condition_patient_counts = conditions_meta.get("condition_patient_counts", {}) or {}
    h3_cells = h3_ref.get("h3_cells", {}) or {}
    generated_at = conditions_meta.get("generated_at") or datetime.now(timezone.utc).isoformat()
    h3_resolution = h3_ref.get("h3_resolution")
    age_groups = conditions_meta.get("age_groups", []) or []
    genders = conditions_meta.get("genders", []) or []

    # 1) Global overview document
    db_client.collection("gold_overview").document("current").set(
        {
            "generated_at": generated_at,
            "h3_resolution": h3_resolution,
            "total_conditions": len(condition_codes),
            "total_h3_cells": len(h3_cells),
            "condition_codes": condition_codes,
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    # 2) Condition metadata
    for condition_code in condition_codes:
        doc_id = _safe_doc_token(condition_code)
        db_client.collection("gold_conditions").document(doc_id).set(
            {
                "condition_code": str(condition_code),
                "display_name": condition_display_map.get(str(condition_code), str(condition_code)),
                "patient_count": int(condition_patient_counts.get(str(condition_code), 0)),
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )

    # 3) Shared H3 lookup documents (lat/lon stored once per H3 cell).
    batch = db_client.batch()
    pending_ops = 0
    h3_written = 0
    for h3_id, ref in h3_cells.items():
        doc_ref = db_client.collection("gold_h3_cells").document(str(h3_id))
        batch.set(
            doc_ref,
            {
                "h3": str(h3_id),
                "latitude": ref.get("latitude"),
                "longitude": ref.get("longitude"),
                "h3_resolution": h3_resolution,
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
        )
        pending_ops += 1
        h3_written += 1
        if pending_ops >= 450:
            batch, pending_ops = _flush_batch(db_client, batch, pending_ops)

    batch, pending_ops = _flush_batch(db_client, batch, pending_ops)
    logger.info(f"Wrote {h3_written} documents to gold_h3_cells")

    # 4) Daily condition+h3+demographic documents for time-range querying.
    batch = db_client.batch()
    pending_ops = 0
    written_docs = 0
    total_case_count = 0

    for condition_code in condition_codes:
        safe_condition_code = str(condition_code).replace("/", "_").replace(" ", "_")
        condition_parquet = gold_dir / f"{safe_condition_code}.parquet"
        if not condition_parquet.exists():
            condition_parquet = gold_dir / f"{condition_code}.parquet"

        if not condition_parquet.exists():
            logger.warning(f"Condition parquet missing for code {condition_code}: {condition_parquet}")
            continue

        for record in _read_parquet_rows(condition_parquet):
            date_key = str(record.get("date_key") or "")
            h3_id = str(record.get("h3") or "")
            age_group = str(record.get("age_group") or "unknown")
            gender = str(record.get("gender") or "unknown")
            if not date_key or not h3_id:
                continue

            case_count = int(record.get("case_count") or 0)
            total_case_count += case_count

            doc_id = "_".join(
                [
                    _safe_doc_token(condition_code),
                    _safe_doc_token(date_key),
                    _safe_doc_token(h3_id),
                    _safe_doc_token(age_group),
                    _safe_doc_token(gender),
                ]
            )
            doc_ref = db_client.collection("gold_daily_cells").document(doc_id)
            batch.set(
                doc_ref,
                {
                    "condition_code": str(condition_code),
                    "date_key": date_key,
                    "h3": h3_id,
                    "age_group": age_group,
                    "gender": gender,
                    "case_count": case_count,
                    "updated_at": firestore.SERVER_TIMESTAMP,
                },
            )

            pending_ops += 1
            written_docs += 1
            if pending_ops >= 450:
                batch, pending_ops = _flush_batch(db_client, batch, pending_ops)

    batch, pending_ops = _flush_batch(db_client, batch, pending_ops)
    logger.info(f"Wrote {written_docs} documents to gold_daily_cells")

    # 5) Demographic filters metadata document.
    db_client.collection("gold_demographics_summary").document("current").set(
        {
            "age_groups": age_groups,
            "genders": genders,
            "generated_at": generated_at,
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )

    # 6) General overview metrics for frontend/global observability.
    db_client.collection("gold_overview").document("current").set(
        {
            "total_cases": total_case_count,
            "total_cases_overall": total_case_count,
            "last_update_time": datetime.now(timezone.utc).isoformat(),
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )
    logger.info(f"Updated overview metrics: total_cases={total_case_count}")


def main():
    try:
        logger.info("Starting Firebase Firestore pusher")
        if not config.ENABLE_FIREBASE_SYNC:
            logger.info("ENABLE_FIREBASE_SYNC=false: skipping Firestore push")
            return 0

        gold_dir = Path(os.environ.get('GOLD_PATH', '/mnt/data/gold'))
        if not gold_dir.exists():
            logger.warning(f"Gold path does not exist: {gold_dir}")
            return 0

        db_client = init_firestore_client()
        push_gold_to_firestore(db_client, gold_dir)

        logger.info("Firestore pusher completed successfully")
        return 0

    except Exception as e:
        logger.warning(f"Firestore sync unavailable, skipping push: {e}")
        return 0


if __name__ == '__main__':
    sys.exit(main())
