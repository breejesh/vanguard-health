#!/usr/bin/env python
"""
Push Synthea COVID-19 CSV records to a FHIR R4 server.

This script reads local Synthea CSV extracts, selects COVID-related condition rows,
and posts transaction bundles that contain conditional Patient/Encounter upserts and
Condition creates. Source condition timestamps are interpolated into a 2026 window.

Examples:
  python push_synthea_covid19_r4.py --max-records 10000 --dry-run
  python push_synthea_covid19_r4.py --base-url https://r4.smarthealthit.org --max-records 10000
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests


PATIENT_IDENTIFIER_SYSTEM = "http://synthea.mitre.org/patient-id"
ENCOUNTER_IDENTIFIER_SYSTEM = "http://synthea.mitre.org/encounter-id"
CONDITION_IDENTIFIER_SYSTEM = "http://synthea.mitre.org/condition-row-id"
GEO_EXTENSION_URL = "http://hl7.org/fhir/StructureDefinition/geolocation"


@dataclass(frozen=True)
class ConditionRow:
    source_index: int
    start: Optional[datetime]
    stop: Optional[datetime]
    patient_id: str
    encounter_id: str
    code: str
    description: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push Synthea COVID CSV records to FHIR R4")
    parser.add_argument(
        "--data-dir",
        default="10k_synthea_covid19_csv",
        help="Directory containing Synthea CSV files",
    )
    parser.add_argument("--base-url", default="https://r4.smarthealthit.org", help="FHIR R4 base URL")
    parser.add_argument("--max-records", type=int, default=10000, help="Max COVID condition rows to push")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout (seconds)")
    parser.add_argument("--token", default=None, help="Optional Bearer token")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between requests (milliseconds)")
    parser.add_argument("--workers", type=int, default=1, help="Deprecated: script now runs sequentially")
    parser.add_argument("--progress-every", type=int, default=100, help="Print progress every N completed records")
    parser.add_argument("--dry-run", action="store_true", help="Build bundles without pushing")
    return parser.parse_args()


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def remap_datetime(dt: Optional[datetime], src_min: datetime, src_max: datetime, dst_min: datetime, dst_max: datetime) -> datetime:
    if dt is None:
        return dst_max

    src_span = (src_max - src_min).total_seconds()
    if src_span <= 0:
        return dst_max

    pos = (dt - src_min).total_seconds() / src_span
    pos = min(1.0, max(0.0, pos))
    dst_span = (dst_max - dst_min).total_seconds()
    return dst_min + (dst_max - dst_min) * pos if dst_span > 0 else dst_max


def iter_csv(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            yield row


def load_covid_conditions(conditions_csv: Path, max_records: int) -> list[ConditionRow]:
    rows: list[ConditionRow] = []
    for idx, row in enumerate(iter_csv(conditions_csv), start=1):
        description = (row.get("DESCRIPTION") or "").strip()
        if "covid" not in description.lower() and "sars" not in description.lower():
            continue

        patient_id = (row.get("PATIENT") or "").strip()
        encounter_id = (row.get("ENCOUNTER") or "").strip()
        if not patient_id or not encounter_id:
            continue

        rows.append(
            ConditionRow(
                source_index=idx,
                start=parse_datetime(row.get("START") or ""),
                stop=parse_datetime(row.get("STOP") or ""),
                patient_id=patient_id,
                encounter_id=encounter_id,
                code=(row.get("CODE") or "840539006").strip() or "840539006",
                description=description or "COVID-19",
            )
        )

        if len(rows) >= max_records:
            break

    return rows


def load_patients(patients_csv: Path, wanted_patient_ids: set[str]) -> dict[str, Dict[str, str]]:
    out: dict[str, Dict[str, str]] = {}
    for row in iter_csv(patients_csv):
        pid = (row.get("Id") or "").strip()
        if pid and pid in wanted_patient_ids:
            out[pid] = row
    return out


def load_encounters(encounters_csv: Path, wanted_encounter_ids: set[str]) -> dict[str, Dict[str, str]]:
    out: dict[str, Dict[str, str]] = {}
    for row in iter_csv(encounters_csv):
        eid = (row.get("Id") or "").strip()
        if eid and eid in wanted_encounter_ids:
            out[eid] = row
    return out


def build_patient_resource(patient: Dict[str, str]) -> Dict[str, Any]:
    pid = (patient.get("Id") or "").strip()
    first = (patient.get("FIRST") or "").strip()
    last = (patient.get("LAST") or "").strip()
    gender_raw = (patient.get("GENDER") or "").strip().lower()
    gender = gender_raw if gender_raw in {"male", "female", "other", "unknown"} else "unknown"
    birth_date = parse_date(patient.get("BIRTHDATE") or "")

    lat = patient.get("LAT") or ""
    lon = patient.get("LON") or ""
    geo_extension = []
    try:
        lat_val = float(lat)
        lon_val = float(lon)
        geo_extension = [
            {
                "url": GEO_EXTENSION_URL,
                "extension": [
                    {"url": "latitude", "valueDecimal": lat_val},
                    {"url": "longitude", "valueDecimal": lon_val},
                ],
            }
        ]
    except ValueError:
        geo_extension = []

    name_text = " ".join(part for part in [first, last] if part)

    resource: Dict[str, Any] = {
        "resourceType": "Patient",
        "identifier": [{"system": PATIENT_IDENTIFIER_SYSTEM, "value": pid}],
        "name": [{"given": [first] if first else [], "family": last, "text": name_text or pid}],
        "gender": gender,
        "address": [
            {
                "line": [patient.get("ADDRESS") or ""],
                "city": patient.get("CITY") or "",
                "state": patient.get("STATE") or "",
                "postalCode": patient.get("ZIP") or "",
                "country": "US",
                "extension": geo_extension,
            }
        ],
    }

    if birth_date is not None:
        resource["birthDate"] = birth_date.isoformat()

    return resource


def build_encounter_resource(
    encounter: Dict[str, str],
    patient_ref: str,
    start_2026: datetime,
    stop_2026: Optional[datetime],
) -> Dict[str, Any]:
    eid = (encounter.get("Id") or "").strip()
    cls = (encounter.get("ENCOUNTERCLASS") or "AMB").strip().upper() or "AMB"
    code = (encounter.get("CODE") or "").strip()
    description = (encounter.get("DESCRIPTION") or "Encounter").strip() or "Encounter"

    resource: Dict[str, Any] = {
        "resourceType": "Encounter",
        "identifier": [{"system": ENCOUNTER_IDENTIFIER_SYSTEM, "value": eid}],
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": cls,
        },
        "subject": {"reference": patient_ref},
        "period": {"start": to_iso(start_2026)},
        "type": [{"text": description}],
    }

    if code:
        resource["type"][0]["coding"] = [
            {
                "system": "http://www.ama-assn.org/go/cpt",
                "code": code,
                "display": description,
            }
        ]

    if stop_2026 is not None and stop_2026 >= start_2026:
        resource["period"]["end"] = to_iso(stop_2026)

    return resource


def build_condition_resource(
    condition: ConditionRow,
    patient_ref: str,
    encounter_ref: str,
    start_2026: datetime,
    stop_2026: Optional[datetime],
) -> Dict[str, Any]:
    cid = f"{condition.patient_id}:{condition.encounter_id}:{condition.source_index}"
    resource: Dict[str, Any] = {
        "resourceType": "Condition",
        "identifier": [{"system": CONDITION_IDENTIFIER_SYSTEM, "value": cid}],
        "clinicalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": "active",
                }
            ]
        },
        "verificationStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                    "code": "confirmed",
                }
            ]
        },
        "code": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": condition.code,
                    "display": condition.description,
                }
            ],
            "text": condition.description,
        },
        "subject": {"reference": patient_ref},
        "encounter": {"reference": encounter_ref},
        "recordedDate": to_iso(start_2026),
        "onsetDateTime": to_iso(start_2026),
    }

    if stop_2026 is not None and stop_2026 >= start_2026:
        resource["abatementDateTime"] = to_iso(stop_2026)

    return resource


def push_bundle(base_url: str, bundle: Dict[str, Any], timeout: int, token: Optional[str]) -> requests.Response:
    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.post(base_url, data=json.dumps(bundle), headers=headers, timeout=timeout)


def _build_bundle_for_condition(
    condition: ConditionRow,
    patient: Dict[str, str],
    encounter: Dict[str, str],
    src_min: datetime,
    src_max: datetime,
    dst_min: datetime,
    dst_max: datetime,
) -> Dict[str, Any]:
    cond_start_2026 = remap_datetime(condition.start, src_min, src_max, dst_min, dst_max)
    cond_stop_2026 = remap_datetime(condition.stop, src_min, src_max, dst_min, dst_max) if condition.stop else None

    enc_start_raw = parse_datetime(encounter.get("START") or "")
    enc_stop_raw = parse_datetime(encounter.get("STOP") or "")
    enc_start_2026 = remap_datetime(enc_start_raw or condition.start, src_min, src_max, dst_min, dst_max)
    enc_stop_2026 = remap_datetime(enc_stop_raw, src_min, src_max, dst_min, dst_max) if enc_stop_raw else None

    patient_urn = f"urn:uuid:{uuid.uuid4()}"
    encounter_urn = f"urn:uuid:{uuid.uuid4()}"
    condition_urn = f"urn:uuid:{uuid.uuid4()}"

    patient_resource = build_patient_resource(patient)
    encounter_resource = build_encounter_resource(encounter, patient_urn, enc_start_2026, enc_stop_2026)
    condition_resource = build_condition_resource(condition, patient_urn, encounter_urn, cond_start_2026, cond_stop_2026)

    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "fullUrl": patient_urn,
                "resource": patient_resource,
                "request": {
                    "method": "PUT",
                    "url": f"Patient?identifier={PATIENT_IDENTIFIER_SYSTEM}|{condition.patient_id}",
                },
            },
            {
                "fullUrl": encounter_urn,
                "resource": encounter_resource,
                "request": {
                    "method": "PUT",
                    "url": f"Encounter?identifier={ENCOUNTER_IDENTIFIER_SYSTEM}|{condition.encounter_id}",
                },
            },
            {
                "fullUrl": condition_urn,
                "resource": condition_resource,
                "request": {"method": "POST", "url": "Condition"},
            },
        ],
    }


def _process_one_condition(
    idx: int,
    total: int,
    condition: ConditionRow,
    patient: Dict[str, str],
    encounter: Dict[str, str],
    args: argparse.Namespace,
    src_min: datetime,
    src_max: datetime,
    dst_min: datetime,
    dst_max: datetime,
) -> Tuple[int, bool, str]:
    bundle = _build_bundle_for_condition(condition, patient, encounter, src_min, src_max, dst_min, dst_max)

    if args.dry_run:
        return idx, True, "dry-run"

    try:
        response = push_bundle(args.base_url, bundle, args.timeout, args.token)
        if 200 <= response.status_code < 300:
            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)
            return idx, True, f"OK ({response.status_code})"
        return idx, False, f"FAILED ({response.status_code}) {response.text[:200]}"
    except requests.RequestException as exc:
        return idx, False, f"ERROR: {exc}"


def main() -> int:
    args = parse_args()
    if args.max_records <= 0:
        print("max-records must be >= 1")
        return 2
    if args.workers <= 0:
        print("workers must be >= 1")
        return 2
    if args.progress_every <= 0:
        print("progress-every must be >= 1")
        return 2

    data_dir = Path(args.data_dir)
    conditions_csv = data_dir / "conditions.csv"
    encounters_csv = data_dir / "encounters.csv"
    patients_csv = data_dir / "patients.csv"

    for path in [conditions_csv, encounters_csv, patients_csv]:
        if not path.exists():
            print(f"Missing required file: {path}")
            return 2

    print(f"Loading COVID conditions from: {conditions_csv}")
    conditions = load_covid_conditions(conditions_csv, args.max_records)
    if not conditions:
        print("No COVID rows found in conditions.csv")
        return 1

    wanted_patients = {c.patient_id for c in conditions}
    wanted_encounters = {c.encounter_id for c in conditions}
    patients = load_patients(patients_csv, wanted_patients)
    encounters = load_encounters(encounters_csv, wanted_encounters)

    valid_conditions = [
        c for c in conditions if c.patient_id in patients and c.encounter_id in encounters
    ]
    dropped = len(conditions) - len(valid_conditions)
    if not valid_conditions:
        print("No valid rows after joining to patient/encounter tables")
        return 1

    source_dates = [c.start for c in valid_conditions if c.start is not None]
    if not source_dates:
        source_dates = [datetime.now(timezone.utc)]
    src_min = min(source_dates)
    src_max = max(source_dates)

    now_utc = datetime.now(timezone.utc)
    dst_min = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    dst_max = now_utc if now_utc.year == 2026 else datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

    print(f"FHIR base URL: {args.base_url}")
    print(f"Selected COVID rows: {len(conditions)}")
    print(f"Rows joined and ready: {len(valid_conditions)}")
    if dropped:
        print(f"Rows dropped due to missing patient/encounter: {dropped}")
    print(f"Time remap source window: {src_min.isoformat()} -> {src_max.isoformat()}")
    print(f"Time remap target window: {dst_min.isoformat()} -> {dst_max.isoformat()}")
    if args.workers > 1:
        print(f"workers={args.workers} requested, but sequential mode is enforced; using workers=1")
    print("Workers: 1 (sequential)")
    if args.dry_run:
        print("Mode: dry-run (no HTTP requests)")

    success = 0
    failed = 0
    sample_bundle: Optional[Dict[str, Any]] = None
    total = len(valid_conditions)
    start_time = time.time()

    # Build one sample bundle for dry-run visibility.
    first_condition = valid_conditions[0]
    sample_bundle = _build_bundle_for_condition(
        first_condition,
        patients[first_condition.patient_id],
        encounters[first_condition.encounter_id],
        src_min,
        src_max,
        dst_min,
        dst_max,
    )

    def _print_progress(done: int) -> None:
        elapsed = max(time.time() - start_time, 1e-9)
        rate = done / elapsed
        remaining = total - done
        eta_seconds = remaining / rate if rate > 0 else 0
        print(
            f"[progress] {done}/{total} done | success={success} failed={failed} "
            f"| rate={rate:.2f}/s | eta={eta_seconds:.1f}s"
        )

    completed = 0
    last_progress_print = 0
    for idx, condition in enumerate(valid_conditions, start=1):
        try:
            _, ok, status = _process_one_condition(
                idx,
                total,
                condition,
                patients[condition.patient_id],
                encounters[condition.encounter_id],
                args,
                src_min,
                src_max,
                dst_min,
                dst_max,
            )
            if ok:
                success += 1
            else:
                failed += 1
                print(f"[{idx}/{total}] {status}")
        except Exception as exc:
            failed += 1
            print(f"[{idx}/{total}] ERROR: {exc}")

        completed += 1
        if completed - last_progress_print >= args.progress_every or completed == total:
            _print_progress(completed)
            last_progress_print = completed

    if args.dry_run and sample_bundle is not None:
        print("--- Dry run sample bundle ---")
        print(json.dumps(sample_bundle, indent=2)[:6000])

    print("--- Summary ---")
    print(f"Success: {success}")
    print(f"Failed : {failed}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
