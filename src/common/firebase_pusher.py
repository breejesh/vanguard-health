"""
Push Gold outputs to Google Firestore.

This is run as a standalone Kubernetes Job/Pod after Silver->Gold completes.
"""
import sys
import os
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple

import firebase_admin
from firebase_admin import credentials, firestore

from src.common.logger import setup_logger
from src.common.config import get_config

logger = setup_logger(__name__)
config = get_config()


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def iter_jsonl(path: Path) -> Iterable[Dict]:
    if not path.exists():
        return
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except Exception:
                logger.warning(f"Skipping invalid JSON line in {path}")


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
    conditions_meta = load_json(gold_dir / "_conditions.json")
    h3_ref = load_json(gold_dir / "_h3_reference.json")

    condition_codes = conditions_meta.get("conditions", []) or []
    condition_display_map = conditions_meta.get("condition_display_map", {}) or {}
    condition_patient_counts = conditions_meta.get("condition_patient_counts", {}) or {}
    h3_cells = h3_ref.get("h3_cells", {}) or {}
    generated_at = conditions_meta.get("generated_at") or datetime.now(timezone.utc).isoformat()
    h3_resolution = h3_ref.get("h3_resolution")

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
        db_client.collection("gold_conditions").document(str(condition_code)).set(
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

    # 4) Daily condition+h3 documents for time-range querying.
    batch = db_client.batch()
    pending_ops = 0
    written_docs = 0
    total_case_count = 0

    for condition_code in condition_codes:
        condition_dir = gold_dir / str(condition_code)
        if not condition_dir.exists() or not condition_dir.is_dir():
            logger.warning(f"Condition directory missing for code {condition_code}: {condition_dir}")
            continue

        for date_file in sorted(condition_dir.glob("*.jsonl")):
            date_key = date_file.stem
            for record in iter_jsonl(date_file):
                h3_id = record.get("h3")
                if not h3_id:
                    continue

                case_count = int(record.get("case_count", 0))
                total_case_count += case_count

                doc_id = f"{condition_code}_{date_key}_{h3_id}"
                doc_ref = db_client.collection("gold_daily_cells").document(doc_id)
                batch.set(
                    doc_ref,
                    {
                        "condition_code": str(condition_code),
                        "date_key": date_key,
                        "h3": h3_id,
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

    # 5) General overview metrics for frontend/global observability.
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
