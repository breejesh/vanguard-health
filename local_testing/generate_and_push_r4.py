#!/usr/bin/env python
"""
Generate synthetic FHIR R4 resources locally and push them to an R4 server.

This script creates linked Patient, Encounter, and Condition resources and sends
one transaction Bundle per synthetic patient.

Examples:
  python local_testing/generate_and_push_r4.py --count 10
  python local_testing/generate_and_push_r4.py --base-url https://r4.smarthealthit.org --count 3 --dry-run
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import requests

FIRST_NAMES = [
    "Ava", "Liam", "Noah", "Emma", "Olivia", "Sophia", "Mason", "Lucas", "Mia", "Ethan",
]
LAST_NAMES = [
    "Abbott", "Baker", "Carter", "Diaz", "Evans", "Foster", "Gray", "Hughes", "Irwin", "Jones",
]
CITIES = [
    ("Framingham", "Massachusetts", "01701"),
    ("Ipswich", "Massachusetts", "01938"),
    ("Franklin Town", "Massachusetts", "02038"),
    ("Newton", "Massachusetts", "02458"),
]
CONDITIONS = [
    ("444814009", "Viral sinusitis (disorder)"),
    ("19941000", "Primary open angle glaucoma (test)"),
    ("44054006", "Type 2 diabetes mellitus"),
]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_transaction_bundle(seed: int | None = None) -> Dict[str, Any]:
    if seed is not None:
        random.seed(seed)

    now = datetime.now(timezone.utc)
    birth = datetime(now.year - random.randint(20, 75), random.randint(1, 12), random.randint(1, 28), tzinfo=timezone.utc)
    encounter_start = now - timedelta(days=random.randint(1, 90))

    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    gender = random.choice(["male", "female"])
    city, state, zipcode = random.choice(CITIES)
    code, display = random.choice(CONDITIONS)

    lat = round(random.uniform(42.0, 42.8), 6)
    lon = round(random.uniform(-71.9, -70.7), 6)

    patient_urn = f"urn:uuid:{uuid.uuid4()}"
    encounter_urn = f"urn:uuid:{uuid.uuid4()}"
    condition_urn = f"urn:uuid:{uuid.uuid4()}"

    patient = {
        "resourceType": "Patient",
        "name": [{"given": [first], "family": last, "text": f"{first} {last}"}],
        "gender": gender,
        "birthDate": birth.strftime("%Y-%m-%d"),
        "telecom": [{"system": "phone", "value": f"555-{random.randint(100,999)}-{random.randint(1000,9999)}"}],
        "address": [
            {
                "city": city,
                "state": state,
                "postalCode": zipcode,
                "country": "US",
                "extension": [
                    {
                        "url": "http://hl7.org/fhir/StructureDefinition/geolocation",
                        "extension": [
                            {"url": "latitude", "valueDecimal": lat},
                            {"url": "longitude", "valueDecimal": lon},
                        ],
                    }
                ],
            }
        ],
    }

    encounter = {
        "resourceType": "Encounter",
        "status": "finished",
        "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB"},
        "subject": {"reference": patient_urn},
        "period": {"start": _iso(encounter_start), "end": _iso(encounter_start + timedelta(hours=1))},
    }

    condition = {
        "resourceType": "Condition",
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]
        },
        "verificationStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed"}]
        },
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": code, "display": display}],
            "text": display,
        },
        "subject": {"reference": patient_urn},
        "encounter": {"reference": encounter_urn},
        "recordedDate": _iso(now),
    }

    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {"fullUrl": patient_urn, "resource": patient, "request": {"method": "POST", "url": "Patient"}},
            {"fullUrl": encounter_urn, "resource": encounter, "request": {"method": "POST", "url": "Encounter"}},
            {"fullUrl": condition_urn, "resource": condition, "request": {"method": "POST", "url": "Condition"}},
        ],
    }


def push_bundle(base_url: str, bundle: Dict[str, Any], timeout: int, token: str | None) -> requests.Response:
    headers = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.post(base_url, headers=headers, data=json.dumps(bundle), timeout=timeout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and push synthetic FHIR R4 data")
    parser.add_argument("--base-url", default="https://r4.smarthealthit.org", help="FHIR R4 base URL")
    parser.add_argument("--count", type=int, default=1, help="Number of synthetic bundles to push")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--token", default=None, help="Optional Bearer token")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed")
    parser.add_argument("--dry-run", action="store_true", help="Print sample bundle(s) without pushing")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.count <= 0:
        print("count must be >= 1")
        return 2

    success = 0
    failed = 0

    print(f"Target FHIR base URL: {args.base_url}")
    print(f"Bundles to generate: {args.count}")

    for i in range(args.count):
        bundle_seed = None if args.seed is None else args.seed + i
        bundle = build_transaction_bundle(seed=bundle_seed)

        if args.dry_run:
            if i == 0:
                print("Sample bundle:")
                print(json.dumps(bundle, indent=2))
            success += 1
            continue

        try:
            response = push_bundle(args.base_url, bundle, args.timeout, args.token)
            if 200 <= response.status_code < 300:
                success += 1
                print(f"[{i + 1}/{args.count}] pushed OK ({response.status_code})")
            else:
                failed += 1
                print(f"[{i + 1}/{args.count}] push FAILED ({response.status_code})")
                print(response.text[:800])
        except requests.RequestException as exc:
            failed += 1
            print(f"[{i + 1}/{args.count}] push ERROR: {exc}")

    print("--- Summary ---")
    print(f"Success: {success}")
    print(f"Failed : {failed}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
