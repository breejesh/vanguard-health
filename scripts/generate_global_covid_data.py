#!/usr/bin/env python3
"""
Generate multi-condition global case data using coordinates from dataset/worldcities.csv.
"""

import csv
import json
import math
import os
import random
from datetime import datetime, timedelta

import pandas as pd

random.seed(42)


def load_world_cities(csv_path, max_cities=10_000, min_population=50_000):
    """Load world cities used as coordinate anchors for synthetic case generation."""
    cities = []
    with open(csv_path, "r", encoding="utf-8") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            try:
                lat = float(row.get("lat", "0") or 0)
                lon = float(row.get("lng", "0") or 0)
                pop = int(float(row.get("population", "0") or 0))
            except (TypeError, ValueError):
                continue

            if pop < min_population:
                continue
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue

            cities.append(
                {
                    "city": row.get("city", ""),
                    "country": row.get("country", ""),
                    "iso2": row.get("iso2", ""),
                    "lat": lat,
                    "lon": lon,
                    "population": pop,
                }
            )

    cities.sort(key=lambda item: item["population"], reverse=True)
    return cities[:max_cities]


def generate_h3_cells_from_cities(cities):
    """Create deterministic H3-like ids, one per world city coordinate."""
    h3_cells = {}
    for idx, city in enumerate(cities):
        h3_id = f"842a{idx:06x}ffffffff"
        h3_cells[h3_id] = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "city": city["city"],
            "country": city["country"],
            "iso2": city["iso2"],
            "population": city["population"],
        }
    return h3_cells


def normalize_weights(weights):
    total = sum(weights)
    if total <= 0:
        return [1.0 / len(weights)] * len(weights)
    return [value / total for value in weights]


def allocate_integer_counts(total, weights):
    """Allocate integer counts that sum exactly to total using weighted remainders."""
    if total <= 0:
        return [0] * len(weights)

    normalized = normalize_weights(weights)
    raw = [normalized[idx] * total for idx in range(len(normalized))]
    base = [int(math.floor(value)) for value in raw]
    remainder = total - sum(base)

    if remainder > 0:
        order = sorted(range(len(raw)), key=lambda idx: (raw[idx] - base[idx]), reverse=True)
        for idx in order[:remainder]:
            base[idx] += 1

    return base


def build_day_weights(num_days, profile):
    """Return normalized day weights by condition profile."""
    weights = []
    for day in range(num_days):
        t = day / max(num_days - 1, 1)
        if profile == "covid":
            # Broad single wave.
            primary = math.exp(-((t - 0.58) ** 2) / (2 * (0.20 ** 2)))
            weight = 0.40 + 1.30 * primary
        elif profile == "synthea26":
            # Relatively flat with gradual rise and taper.
            weight = 0.85 + 0.35 * math.sin(math.pi * t)
        elif profile == "influenza":
            # More seasonal with strong early peak and smaller late peak.
            primary = math.exp(-((t - 0.28) ** 2) / (2 * (0.13 ** 2)))
            secondary = math.exp(-((t - 0.78) ** 2) / (2 * (0.15 ** 2)))
            weight = 0.50 + 1.60 * primary + 0.55 * secondary
        else:
            weight = 1.0
        weights.append(max(weight, 0.0001))

    return normalize_weights(weights)


def build_city_activation_days(num_cities, num_days, profile):
    """Spread city activation across the timeline so active hotspot cells rise during pulse playback."""
    if num_cities <= 0 or num_days <= 0:
        return []

    # Slightly front-load larger-condition profiles while still activating cities through the end.
    curve_by_profile = {
        "covid": 0.95,
        "synthea26": 1.05,
        "influenza": 1.10,
    }
    exponent = curve_by_profile.get(profile, 1.0)
    max_day_index = num_days - 1

    activation_days = []
    for idx in range(num_cities):
        ratio = idx / max(num_cities - 1, 1)
        activation_day = int(round((ratio ** exponent) * max_day_index))
        activation_days.append(min(max(activation_day, 0), max_day_index))

    return activation_days


def build_city_activity_window(city_index, activation_day, num_days, profile):
        """Return an active date window for a city to avoid flat hotspot counts across all filters."""
        if activation_day >= num_days:
                return num_days - 1, num_days - 1

        profile_seed = {
                "covid": 11,
                "synthea26": 29,
                "influenza": 47,
        }.get(profile, 73)
        rng = random.Random((city_index + 1) * 7919 + profile_seed)

        remaining_days = num_days - activation_day
        min_span = min(remaining_days, 7)

        span_ratio_by_profile = {
                "covid": (0.22, 0.55),
                "synthea26": (0.30, 0.70),
                "influenza": (0.18, 0.45),
        }
        low_ratio, high_ratio = span_ratio_by_profile.get(profile, (0.25, 0.60))
        span = int(round(remaining_days * rng.uniform(low_ratio, high_ratio)))
        span = max(min_span, min(span, remaining_days))

        # Keep many cities active near the latest date, while others fade earlier.
        stay_active_until_end_probability = 0.72
        if rng.random() < stay_active_until_end_probability:
            end_day = num_days - 1
            start_day = max(activation_day, end_day - span + 1)
        else:
            latest_start = max(activation_day, num_days - span)
            start_day = rng.randint(activation_day, latest_start)
            end_day = min(num_days - 1, start_day + span - 1)

        return start_day, end_day


def build_city_case_totals(h3_items, total_cases, us_cases=None):
    """Allocate condition totals across cities, optionally forcing US/non-US split."""
    if us_cases is None:
        weights = [max(cell["population"], 1) * random.uniform(0.92, 1.08) for _, cell in h3_items]
        return allocate_integer_counts(total_cases, weights)

    us_indexes = [idx for idx, (_, cell) in enumerate(h3_items) if str(cell.get("iso2", "")).upper() == "US"]
    non_us_indexes = [idx for idx in range(len(h3_items)) if idx not in us_indexes]

    us_total = min(max(us_cases, 0), total_cases)
    non_us_total = total_cases - us_total
    totals = [0] * len(h3_items)

    if us_indexes:
        us_weights = [max(h3_items[idx][1]["population"], 1) * random.uniform(0.92, 1.08) for idx in us_indexes]
        us_alloc = allocate_integer_counts(us_total, us_weights)
        for alloc_idx, city_idx in enumerate(us_indexes):
            totals[city_idx] = us_alloc[alloc_idx]

    if non_us_indexes:
        non_us_weights = [max(h3_items[idx][1]["population"], 1) * random.uniform(0.92, 1.08) for idx in non_us_indexes]
        non_us_alloc = allocate_integer_counts(non_us_total, non_us_weights)
        for alloc_idx, city_idx in enumerate(non_us_indexes):
            totals[city_idx] += non_us_alloc[alloc_idx]

    return totals


def generate_condition_records(condition_config, h3_cells, start_date, num_days, age_groups, genders):
    """Generate daily records by date, city H3 anchor, and demographics."""
    h3_items = list(h3_cells.items())
    total_cases_target = int(condition_config["target_cases"])
    day_weights = build_day_weights(num_days, condition_config["profile"])

    age_weight_map = {
        "0-17": 0.18,
        "18-34": 0.31,
        "35-54": 0.29,
        "55+": 0.22,
    }
    gender_weight_map = {
        "male": 0.34,
        "female": 0.34,
        "other": 0.32,
    }

    demographics = [(age_group, gender) for age_group in age_groups for gender in genders]
    demographic_weights = [
        age_weight_map.get(age_group, 1 / max(len(age_groups), 1))
        * gender_weight_map.get(gender, 1 / max(len(genders), 1))
        for age_group, gender in demographics
    ]
    demographic_weights = normalize_weights(demographic_weights)

    city_totals = build_city_case_totals(
        h3_items,
        total_cases_target,
        us_cases=condition_config.get("us_cases"),
    )
    city_activation_days = build_city_activation_days(
        num_cities=len(h3_items),
        num_days=num_days,
        profile=condition_config.get("profile", ""),
    )

    rows_by_date = {}
    all_rows = []
    total_generated = 0

    for item_idx, ((h3_id, _cell), city_total) in enumerate(zip(h3_items, city_totals)):
        if city_total <= 0:
            continue

        activation_day = city_activation_days[item_idx]
        activity_start_day, activity_end_day = build_city_activity_window(
            city_index=item_idx,
            activation_day=activation_day,
            num_days=num_days,
            profile=condition_config.get("profile", ""),
        )

        city_day_weights = day_weights[activity_start_day:activity_end_day + 1]
        if not city_day_weights:
            continue

        city_day_totals = allocate_integer_counts(city_total, city_day_weights)
        for offset, day_total in enumerate(city_day_totals):
            if day_total <= 0:
                continue

            day_idx = activity_start_day + offset

            date_key = (start_date + timedelta(days=day_idx)).strftime("%Y-%m-%d")
            rows_for_date = rows_by_date.setdefault(date_key, [])
            demographic_totals = allocate_integer_counts(day_total, demographic_weights)

            for demo_idx, case_count in enumerate(demographic_totals):
                if case_count <= 0:
                    continue

                age_group, gender = demographics[demo_idx]
                record = {
                    "condition_code": condition_config["code"],
                    "date_key": date_key,
                    "date": date_key,
                    "h3": h3_id,
                    "age_group": age_group,
                    "gender": gender,
                    "case_count": int(case_count),
                }
                rows_for_date.append(record)
                all_rows.append(record)
                total_generated += int(case_count)

        if (item_idx + 1) % 200 == 0:
            print(f"  Processed {item_idx + 1}/{len(h3_items)} city cells for {condition_config['display']}")

    return rows_by_date, all_rows, total_generated


def write_condition_outputs(gold_dir, condition_code, rows_by_date, all_rows, start_date, num_days):
    """Write daily JSONL files and a unified parquet file per condition."""
    condition_dir = os.path.join(gold_dir, condition_code)
    os.makedirs(condition_dir, exist_ok=True)

    for day_idx in range(num_days):
        date_key = (start_date + timedelta(days=day_idx)).strftime("%Y-%m-%d")
        jsonl_path = os.path.join(condition_dir, f"{date_key}.jsonl")
        with open(jsonl_path, "w", encoding="utf-8") as file_obj:
            for row in rows_by_date.get(date_key, []):
                file_obj.write(json.dumps(row) + "\n")

    parquet_path = os.path.join(gold_dir, f"{condition_code}.parquet")
    frame = pd.DataFrame(all_rows)

    if not frame.empty:
        frame["condition_code"] = frame["condition_code"].astype("category")
        frame["date_key"] = frame["date_key"].astype("string")
        frame["date"] = frame["date"].astype("string")
        frame["h3"] = frame["h3"].astype("string")
        frame["age_group"] = frame["age_group"].astype("category")
        frame["gender"] = frame["gender"].astype("category")
        frame["case_count"] = frame["case_count"].astype("int32")

    frame.to_parquet(parquet_path, engine="pyarrow", index=False, compression="snappy")
    return parquet_path


def write_metadata_files(gold_dir, condition_configs, case_totals, h3_cells, age_groups, genders):
    generated_at = datetime.now().isoformat()

    condition_codes = [item["code"] for item in condition_configs]
    condition_display_map = {item["code"]: item["display"] for item in condition_configs}
    condition_patient_counts = {item["code"]: int(case_totals[item["code"]]) for item in condition_configs}

    conditions_metadata = {
        "generated_at": generated_at,
        "conditions": condition_codes,
        "condition_display_map": condition_display_map,
        "condition_patient_counts": condition_patient_counts,
        "total_unique": len(condition_codes),
        "age_groups": age_groups,
        "genders": genders,
    }

    with open(os.path.join(gold_dir, "_conditions.json"), "w", encoding="utf-8") as file_obj:
        json.dump(conditions_metadata, file_obj, indent=2)

    conditions_parquet_rows = [
        {
            "condition_code": code,
            "condition_display": condition_display_map.get(code, code),
            "patient_count": int(condition_patient_counts.get(code, 0)),
            "generated_at": generated_at,
            "total_unique": len(condition_codes),
            "age_groups_json": json.dumps(age_groups),
            "genders_json": json.dumps(genders),
        }
        for code in condition_codes
    ]
    pd.DataFrame(conditions_parquet_rows).to_parquet(
        os.path.join(gold_dir, "_conditions.parquet"),
        engine="pyarrow",
        index=False,
        compression="snappy",
    )

    h3_reference = {
        "generated_at": generated_at,
        "h3_resolution": 4,
        "h3_cells": {
            h3_id: {
                "latitude": float(cell["latitude"]),
                "longitude": float(cell["longitude"]),
            }
            for h3_id, cell in h3_cells.items()
        },
        "total_unique": len(h3_cells),
    }
    with open(os.path.join(gold_dir, "_h3_reference.json"), "w", encoding="utf-8") as file_obj:
        json.dump(h3_reference, file_obj, indent=2)

    h3_rows = [
        {
            "h3": h3_id,
            "latitude": float(cell["latitude"]),
            "longitude": float(cell["longitude"]),
            "generated_at": generated_at,
            "total_unique": int(len(h3_cells)),
        }
        for h3_id, cell in h3_cells.items()
    ]
    pd.DataFrame(h3_rows).to_parquet(
        os.path.join(gold_dir, "_h3_reference.parquet"),
        engine="pyarrow",
        index=False,
        compression="snappy",
    )


def resolve_default_start_date(num_days):
    """Return an inclusive window start date ending today."""
    today = datetime.now().date()
    return today - timedelta(days=max(num_days - 1, 0))


def generate_dummy_data(base_date=None, num_days=181, city_limit=10_000, min_city_population=50_000):
    """Generate global condition parquet/jsonl outputs from dataset city coordinates."""
    dataset_dir = "local_testing/dataset"
    world_cities_path = os.path.join(dataset_dir, "worldcities.csv")
    gold_dir = "local_testing/data/gold"
    os.makedirs(gold_dir, exist_ok=True)

    condition_configs = [
        {
            "code": "840539006",
            "display": "COVID-19",
            "target_cases": 10_000_000,
            "profile": "covid",
        },
        {
            "code": "Synthea-26",
            "display": "Synthea-26",
            "target_cases": 5_000_000,
            "us_cases": 3_000_000,
            "profile": "synthea26",
        },
        {
            "code": "influenza",
            "display": "Influenza",
            "target_cases": 50_000_000,
            "profile": "influenza",
        },
    ]

    age_groups = ["0-17", "18-34", "35-54", "55+"]
    genders = ["male", "female", "other"]

    if base_date:
        start_date = datetime.fromisoformat(base_date.replace("Z", "+00:00")).date()
    else:
        start_date = resolve_default_start_date(num_days)
    cities = load_world_cities(
        world_cities_path,
        max_cities=city_limit,
        min_population=min_city_population,
    )
    h3_cells = generate_h3_cells_from_cities(cities)

    print(f"Loaded {len(cities)} cities from {world_cities_path}")
    print(f"Generated {len(h3_cells)} H3 city cells")

    condition_totals = {}

    for condition in condition_configs:
        print("\n" + "=" * 70)
        print(f"Generating {condition['display']} ({condition['code']})")
        print(f"Target total cases: {condition['target_cases']:,}")
        if condition.get("us_cases") is not None:
            print(f"Target US cases:    {condition['us_cases']:,}")

        rows_by_date, all_rows, generated_total = generate_condition_records(
            condition,
            h3_cells,
            start_date,
            num_days,
            age_groups,
            genders,
        )

        parquet_path = write_condition_outputs(
            gold_dir,
            condition["code"],
            rows_by_date,
            all_rows,
            start_date,
            num_days,
        )

        condition_totals[condition["code"]] = generated_total
        print(f"Generated total cases: {generated_total:,}")
        print(f"Wrote parquet file: {parquet_path}")

    write_metadata_files(gold_dir, condition_configs, condition_totals, h3_cells, age_groups, genders)

    print("\n" + "=" * 70)
    print("Generation complete")
    print("=" * 70)
    for condition in condition_configs:
        code = condition["code"]
        print(f"{condition['display']:<14} ({code}): {condition_totals[code]:,} cases")
    print(f"H3 cells: {len(h3_cells):,}")
    print(f"Date range: {start_date.isoformat()} to {(start_date + timedelta(days=num_days - 1)).isoformat()}")
    print(f"Output directory: {gold_dir}")


if __name__ == "__main__":
    generate_dummy_data(num_days=181, city_limit=10_000, min_city_population=50_000)
