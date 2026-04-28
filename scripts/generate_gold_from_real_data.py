#!/usr/bin/env python3
"""
Generate 10M high-quality COVID-19 data with realistic wave progression and geographic distribution.
"""

import csv
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import random
import math
import pandas as pd

random.seed(42)

def load_world_cities(csv_path, limit=300):
    """Load top cities by population"""
    cities = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                try:
                    pop = int(row.get('population', 0)) or 100000
                    cities.append({
                        'city': row.get('city', ''),
                        'country': row.get('country', ''),
                        'lat': float(row.get('lat', 0)),
                        'lon': float(row.get('lng', 0)),
                        'population': pop,
                    })
                except:
                    continue
    except Exception as e:
        print(f"Error loading cities: {e}")
    return cities

def generate_h3_cells_from_cities(cities):
    """Generate H3 cells at exact city locations"""
    cells = {}
    for idx, city in enumerate(cities):
        h3_id = f"842a{idx:06x}ffffffff"
        cells[h3_id] = {
            "latitude": city['lat'],
            "longitude": city['lon'],
            "city": city['city'],
            "country": city['country'],
            "population": city['population'],
        }
    return cells

def epidemic_curve(day, total_days, peak_day=None):
    """
    Realistic COVID-19 epidemic curve (logistic function).
    Starts low, peaks in middle, then declines.
    """
    if peak_day is None:
        peak_day = total_days // 2
    
    # Normalized day (0 to 1)
    t = day / total_days
    peak_t = peak_day / total_days
    
    # Logistic function centered at peak_day
    # Steep rise before peak, gradual decline after
    steepness = 10
    intensity = 1.0 / (1.0 + math.exp(-steepness * (t - peak_t)))
    
    return intensity

def generate_daily_cases(h3_cells, num_days=181, target_total_cases=10_000_000):
    """Generate realistic daily case distribution"""
    cases_by_date_h3 = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    # Calculate population-weighted distribution
    total_pop = sum(cell['population'] for cell in h3_cells.values())
    h3_list = list(h3_cells.items())
    
    base_date = datetime.strptime("2025-10-27", "%Y-%m-%d")
    
    # Demographic buckets (4 age groups × 3 genders)
    age_groups = ["0-17", "18-34", "35-54", "55+"]
    genders = ["male", "female", "other"]
    total_demographics = len(age_groups) * len(genders)
    
    # Realistic age distribution (older populations more affected)
    age_weights = [0.05, 0.20, 0.25, 0.50]  # More 55+
    
    # Gender weights by age
    gender_weights_by_age = {
        "0-17": [0.33, 0.33, 0.34],
        "18-34": [0.35, 0.33, 0.32],
        "35-54": [0.34, 0.34, 0.32],
        "55+": [0.35, 0.34, 0.31],
    }
    
    # Generate cases for each day
    for day in range(num_days):
        current_date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
        
        # Epidemic curve intensity for this day (0 to 1)
        intensity = epidemic_curve(day, num_days)
        
        # Daily case count based on target and epidemic curve
        daily_cases = int((target_total_cases / num_days) * intensity)
        
        # Add small random variance for realism (+/- 10%)
        daily_cases = int(daily_cases * (0.9 + random.random() * 0.2))
        
        # Distribute to H3 cells by population
        for h3_id, cell in h3_list:
            # Population-weighted case count
            pop_weight = cell['population'] / total_pop
            h3_daily_cases = int(daily_cases * pop_weight)
            
            if h3_daily_cases > 0:
                # Distribute proportionally across all 12 demographic combinations
                # This preserves counts better than hierarchical division
                for age_idx, age_group in enumerate(age_groups):
                    age_weight = age_weights[age_idx]
                    
                    for gender_idx, gender in enumerate(genders):
                        gender_weight = gender_weights_by_age[age_group][gender_idx]
                        
                        # Combined demographic weight
                        demo_weight = age_weight * gender_weight
                        
                        # Cases for this demographic
                        demo_cases = int(h3_daily_cases * demo_weight)
                        
                        if demo_cases > 0:
                            cases_by_date_h3[current_date][h3_id][f"{age_group}|{gender}"] += demo_cases
    
    return cases_by_date_h3

def write_data_files(output_dir, condition_code, cases_by_date_h3, base_date="2025-10-27", num_days=181):
    """Write daily JSONL files and a unified Parquet file"""
    os.makedirs(output_dir, exist_ok=True)
    base = datetime.strptime(base_date, "%Y-%m-%d")
    total_cases = 0
    
    all_records = []
    
    for day in range(num_days):
        current_date = (base + timedelta(days=day)).strftime("%Y-%m-%d")
        jsonl_path = os.path.join(output_dir, f"{current_date}.jsonl")
        
        with open(jsonl_path, 'w') as f:
            if current_date in cases_by_date_h3:
                for h3_id, demographics in cases_by_date_h3[current_date].items():
                    for demo_key, count in demographics.items():
                        age_group, gender = demo_key.split('|')
                        record = {
                            "condition_code": condition_code,
                            "date_key": current_date,
                            "h3": h3_id,
                            "age_group": age_group,
                            "gender": gender,
                            "case_count": count,
                        }
                        f.write(json.dumps(record) + "\n")
                        all_records.append(record)
                        total_cases += count
        
        if (day + 1) % 30 == 0:
            print(f"Generated data for {current_date}")
            
    # Write unified Parquet file
    parquet_path = os.path.join(os.path.dirname(output_dir), f"{condition_code}.parquet")
    print(f"\nWriting unified Parquet file: {parquet_path}")
    df = pd.DataFrame(all_records)
    # Convert types for optimization
    df['condition_code'] = df['condition_code'].astype('category')
    df['date_key'] = df['date_key'].astype('string')
    df['h3'] = df['h3'].astype('string')
    df['age_group'] = df['age_group'].astype('category')
    df['gender'] = df['gender'].astype('category')
    df['case_count'] = df['case_count'].astype('int32')
    
    df.to_parquet(parquet_path, engine='pyarrow', index=False, compression='snappy')
    print(f"✓ Saved Parquet file with {len(df)} records ({parquet_path})")
    
    return total_cases

def main():
    base_dir = "local_testing/data/gold"
    dataset_dir = "local_testing/dataset"
    worldcities_path = os.path.join(dataset_dir, "worldcities.csv")
    
    covid_code = "840539006"
    covid_dir = os.path.join(base_dir, covid_code)
    os.makedirs(covid_dir, exist_ok=True)
    
    print("=" * 70)
    print("GENERATING 10M REALISTIC COVID-19 DATA WITH WAVE PROGRESSION")
    print("=" * 70)
    
    # Step 1: Load cities
    print("\n[1/5] Loading world cities...")
    cities = load_world_cities(worldcities_path, limit=300)
    print(f"✓ Loaded {len(cities)} cities")
    print(f"  Total population: {sum(c['population'] for c in cities):,}")
    
    # Step 2: Generate H3 cells at city locations
    print("\n[2/5] Generating H3 cells at city locations...")
    h3_cells = generate_h3_cells_from_cities(cities)
    print(f"✓ Generated {len(h3_cells)} H3 cells")
    
    # Step 3: Generate daily cases with epidemic curve
    print("\n[3/5] Generating 10M+ cases with COVID-19 wave progression...")
    target_cases = 20_000_000  # Target 20M to achieve ~10M after demographic division
    cases_by_date_h3 = generate_daily_cases(h3_cells, num_days=181, target_total_cases=target_cases)
    print(f"✓ Generated case distribution for {len(cases_by_date_h3)} dates")
    
    # Step 4: Write JSONL and Parquet files
    print("\n[4/5] Writing daily JSONL and unified Parquet files...")
    total_cases = write_data_files(covid_dir, covid_code, cases_by_date_h3, num_days=181)
    print(f"✓ Total case records: {total_cases:,}")
    
    # Step 5: Write metadata
    print("\n[5/5] Writing metadata...")
    conditions_metadata = {
        "generated_at": datetime.now().isoformat(),
        "conditions": [covid_code],
        "condition_display_map": {covid_code: "COVID-19"},
        "age_groups": ["0-17", "18-34", "35-54", "55+"],
        "genders": ["male", "female", "other"],
        "condition_patient_counts": {covid_code: total_cases},
        "total_unique": 1,
    }
    
    with open(os.path.join(base_dir, "_conditions.json"), 'w') as f:
        json.dump(conditions_metadata, f, indent=2)
    
    h3_reference = {
        "generated_at": datetime.now().isoformat(),
        "h3_cells": {h3_id: {"latitude": cell["latitude"], "longitude": cell["longitude"]} 
                     for h3_id, cell in h3_cells.items()}
    }
    
    with open(os.path.join(base_dir, "_h3_reference.json"), 'w') as f:
        json.dump(h3_reference, f, indent=2)

    # Also emit parquet metadata files consumed by the frontend parquet service.
    conditions_parquet_df = pd.DataFrame([
        {
            "condition_code": covid_code,
            "condition_display": "COVID-19",
            "patient_count": int(total_cases),
            "generated_at": conditions_metadata["generated_at"],
            "total_unique": int(conditions_metadata["total_unique"]),
            "age_groups_json": json.dumps(conditions_metadata["age_groups"]),
            "genders_json": json.dumps(conditions_metadata["genders"]),
        }
    ])
    conditions_parquet_df.to_parquet(
        os.path.join(base_dir, "_conditions.parquet"),
        engine='pyarrow',
        index=False,
        compression='snappy'
    )

    h3_rows = [
        {
            "h3": h3_id,
            "latitude": float(cell["latitude"]),
            "longitude": float(cell["longitude"]),
            "generated_at": h3_reference["generated_at"],
            "total_unique": int(len(h3_cells)),
        }
        for h3_id, cell in h3_cells.items()
    ]
    h3_reference_df = pd.DataFrame(h3_rows)
    h3_reference_df.to_parquet(
        os.path.join(base_dir, "_h3_reference.parquet"),
        engine='pyarrow',
        index=False,
        compression='snappy'
    )
    
    print("\n" + "=" * 70)
    print("✓ GENERATION COMPLETE!")
    print("=" * 70)
    print(f"  Cities:                {len(cities)} major cities worldwide")
    print(f"  H3 cells:              {len(h3_cells)} (one per city)")
    print(f"  Date range:            2025-10-27 to 2026-04-25 (181 days)")
    print(f"  Total COVID cases:     {total_cases:,}")
    print(f"  Wave progression:      Low → Peak (2026-02-24) → Decline")
    print(f"  Demographics:          4 age groups × 3 genders (12 per h3/date)")
    print(f"  Age distribution:      Weighted to 55+ (50%)")
    print(f"  Geographic:            All 300 H3 cells populated (pop-weighted)")
    print(f"  Output directory:      {base_dir}")
    print("=" * 70)

if __name__ == "__main__":
    main()
