"""
Silver to Gold transformation for geographic disease analysis.

Aggregates conditions by geographic location, condition type, and time
and produces UI-ready JSON outputs for dashboard queries with time filtering.
"""
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
import h3

logger = logging.getLogger(__name__)


def extract_condition_date(condition: dict) -> datetime:
    """Extract onset date from FHIR condition resource, default to now if not found."""
    if 'onsetDateTime' in condition:
        try:
            return datetime.fromisoformat(condition['onsetDateTime'].replace('Z', '+00:00')).replace(tzinfo=None)
        except:
            pass
    return datetime.now()


def extract_location(patient: dict) -> dict:
    """Extract city, state, zipcode, and coordinates from FHIR patient address."""
    location = {
        'city': None,
        'state': None,
        'zipcode': None,
        'country': None,
        'latitude': None,
        'longitude': None
    }
    
    if 'address' in patient and isinstance(patient['address'], list):
        for addr in patient['address']:
            if isinstance(addr, dict):
                location['city'] = addr.get('city', location['city'])
                location['state'] = addr.get('state', location['state'])
                location['zipcode'] = addr.get('postalCode', location['zipcode'])
                location['country'] = addr.get('country', location['country'])
                
                # Extract geolocation if available
                if 'extension' in addr:
                    for ext in addr['extension']:
                        if isinstance(ext, dict) and ext.get('url', '').endswith('geolocation'):
                            if 'extension' in ext:
                                for geo_ext in ext['extension']:
                                    if 'latitude' in geo_ext.get('url', ''):
                                        location['latitude'] = geo_ext.get('valueDecimal')
                                    elif 'longitude' in geo_ext.get('url', ''):
                                        location['longitude'] = geo_ext.get('valueDecimal')
    
    return location


def extract_condition_code(condition: dict) -> str:
    """Extract condition code/display from FHIR condition resource."""
    if 'code' in condition and isinstance(condition['code'], dict):
        if 'text' in condition['code']:
            return condition['code']['text']
        if 'coding' in condition['code'] and isinstance(condition['code']['coding'], list):
            for coding in condition['code']['coding']:
                if isinstance(coding, dict) and 'display' in coding:
                    return coding['display']
                elif isinstance(coding, dict) and 'code' in coding:
                    return coding['code']
    return 'Unknown Condition'


def read_jsonl_file(filepath: Path) -> List[dict]:
    """Read JSONL file and return list of dictionaries."""
    records = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.debug(f"Skipped invalid JSON line: {e}")
    except FileNotFoundError:
        logger.warning(f"File not found: {filepath}")
    return records


def generate_time_filtered_hotspots(conditions_by_location: dict, time_window_hours: int, gold_path: Path, window_name: str) -> dict:
    """
    Generate hotspots and stats for a specific time window (e.g., last 1 week).
    
    Args:
        conditions_by_location: Full dataset with timestamp info
        time_window_hours: Hours to look back (e.g., 168 for 1 week)
        gold_path: Output directory
        window_name: Time window name (e.g., "1w")
    
    Returns:
        Dictionary with counts for this window
    """
    cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
    
    # Filter cases by time window
    filtered_hotspots = []
    window_stats = {
        'total_cases': 0,
        'unique_conditions': set(),
        'affected_regions': set(),
        'unique_cities': set()
    }
    
    max_case_count = 1
    conditions_data = {}  # condition -> {city: count, ...}
    
    for condition, locations_dict in conditions_by_location.items():
        conditions_data[condition] = {}
        for location_name, loc_data in locations_dict.items():
            # Filter dates within time window
            filtered_dates = [d for d in loc_data['dates'] if datetime.fromisoformat(d) > cutoff_time]
            case_count = len(filtered_dates)
            
            if case_count > 0 and loc_data['latitude'] is not None and loc_data['longitude'] is not None:
                window_stats['total_cases'] += case_count
                window_stats['unique_conditions'].add(condition)
                if loc_data['state']:
                    window_stats['affected_regions'].add(loc_data['state'])
                if loc_data['city']:
                    window_stats['unique_cities'].add(loc_data['city'])
                
                conditions_data[condition][location_name] = case_count
                max_case_count = max(max_case_count, case_count)
                
                # Create hotspot feature
                intensity = case_count / max_case_count
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(loc_data['longitude']), float(loc_data['latitude'])]
                    },
                    "properties": {
                        "condition": condition,
                        "location": location_name,
                        "city": loc_data['city'],
                        "state": loc_data['state'],
                        "zipcode": loc_data['zipcode'],
                        "case_count": case_count,
                        "intensity": round(intensity, 3),
                        "time_window": window_name,
                        "generated_at": datetime.now().isoformat()
                    }
                }
                filtered_hotspots.append(feature)
    
    # Save time-window filtered hotspots
    output_file = gold_path / f"disease_hotspots_{window_name}.json"
    geojson = {
        "type": "FeatureCollection",
        "features": filtered_hotspots,
        "properties": {
            "time_window": window_name,
            "cutoff_time": cutoff_time.isoformat(),
            "generated_at": datetime.now().isoformat()
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)
    
    logger.info(f"[OK] Created disease_hotspots_{window_name}.json ({window_stats['total_cases']} cases)")
    
    # Save time-window filtered stats
    output_stats = gold_path / f"disease_stats_{window_name}.json"
    stats = {
        "generated_at": datetime.now().isoformat(),
        "time_window": window_name,
        "cutoff_time": cutoff_time.isoformat(),
        "total_cases": window_stats['total_cases'],
        "unique_conditions": len(window_stats['unique_conditions']),
        "affected_regions": len(window_stats['affected_regions']),
        "unique_cities": len(window_stats['unique_cities'])
    }
    
    with open(output_stats, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    logger.info(f"[OK] Created disease_stats_{window_name}.json")
    
    return window_stats


def transform_silver_to_gold(silver_path, gold_path) -> Tuple[int, str]:
    """
    Transform silver layer to gold layer with daily aggregations partitioned by date and condition.
    
    Output structure (scalable for big data):
    gold/
    ├── 2026-03-22/
    │   ├── Acute_bronchitis/
    │   │   └── data.json (all Acute bronchitis cases on this date)
    │   ├── Flu/
    │   │   └── data.json
    │   └── Normal_pregnancy/
    │       └── data.json
    ├── 2026-03-21/
    │   └── ... (same structure)
    
    Each partition contains daily counts by location and state.
    This enables:
    - Query by date range: load only needed date partitions
    - Query by condition: load only condition partitions
    - Incremental updates: add new dates without reprocessing
    - Distributed processing: each partition processed independently
    
    Args:
        silver_path: Path to silver layer directory (str or Path)
        gold_path: Path to gold layer directory (str or Path, will be created if not exists)
    
    Returns:
        Tuple of (success_code, message)
    """
    # Convert string paths to Path objects
    silver_path = Path(silver_path)
    gold_path = Path(gold_path)
    
    logger.info("="*60)
    logger.info("Silver to Gold Transformation (Partitioned by Date + Condition)")
    logger.info("="*60)
    logger.info(f"Silver Path: {silver_path}")
    logger.info(f"Gold Path: {gold_path}")
    logger.info("Output: Partitioned by date/condition for big data scalability")
    
    # Create gold directory
    gold_path.mkdir(parents=True, exist_ok=True)
    
    # Read silver layer data
    logger.info("")
    logger.info("Reading silver layer data...")
    
    patient_file = silver_path / "Patient_silver" / "data.jsonl"
    condition_file = silver_path / "Condition_silver" / "data.jsonl"
    
    if not patient_file.exists():
        msg = f"Patient file not found: {patient_file}"
        logger.error(f"[ERROR] {msg}")
        return 1, msg
    
    if not condition_file.exists():
        msg = f"Condition file not found: {condition_file}"
        logger.error(f"[ERROR] {msg}")
        return 1, msg
    
    patients = read_jsonl_file(patient_file)
    conditions = read_jsonl_file(condition_file)
    
    logger.info(f"[OK] Read {len(patients)} patients")
    logger.info(f"[OK] Read {len(conditions)} conditions")
    
    # Build patient ID -> location mapping
    patient_locations = {}
    for patient in patients:
        patient_id = patient.get('id')
        if patient_id:
            patient_locations[patient_id] = extract_location(patient)
    
    logger.info(f"[OK] Extracted locations for {len(patient_locations)} patients")
    
    # Aggregate conditions by geography WITH COORDINATES and TIMESTAMPS
    logger.info("")
    logger.info("Aggregating conditions by geography, time, and condition type...")
    
    # Structure: condition -> city_name -> data with count and dates
    conditions_by_location = defaultdict(lambda: defaultdict(lambda: {
        'latitude': None,
        'longitude': None,
        'city': None,
        'state': None,
        'zipcode': None,
        'case_count': 0,
        'dates': []  # Track dates for time-series
    }))
    
    # Time-based aggregations
    conditions_by_state = defaultdict(lambda: defaultdict(int))  # state -> condition -> count
    conditions_by_city = defaultdict(lambda: defaultdict(int))  # city -> condition -> count
    conditions_by_zipcode = defaultdict(lambda: defaultdict(int))  # zipcode -> condition -> count
    conditions_by_condition = defaultdict(lambda: defaultdict(int))  # condition -> state -> count
    all_conditions_set = set()
    
    patient_conditions = defaultdict(list)
    
    for condition in conditions:
        subject = condition.get('subject', {})
        patient_ref = subject.get('reference', '') if isinstance(subject, dict) else ''
        patient_id = patient_ref.split('/')[-1] if '/' in patient_ref else None
        
        if patient_id and patient_id in patient_locations:
            condition_code = extract_condition_code(condition)
            condition_date = extract_condition_date(condition)
            location = patient_locations[patient_id]
            
            patient_conditions[patient_id].append(condition_code)
            all_conditions_set.add(condition_code)
            
            # Create location key
            city_key = f"{location['city']}, {location['state']}" if location['city'] and location['state'] else location['city'] or 'Unknown'
            
            # Aggregate with coordinates for mapping
            loc_data = conditions_by_location[condition_code][city_key]
            loc_data['latitude'] = location['latitude']
            loc_data['longitude'] = location['longitude']
            loc_data['city'] = location['city']
            loc_data['state'] = location['state']
            loc_data['zipcode'] = location['zipcode']
            loc_data['case_count'] += 1
            loc_data['dates'].append(condition_date.isoformat())
            
            # State and condition aggregations
            if location['state']:
                conditions_by_state[condition_code][location['state']] += 1
                conditions_by_condition[condition_code][location['state']] += 1
            if location['city']:
                conditions_by_city[condition_code][location['city']] += 1
            if location['zipcode']:
                conditions_by_zipcode[condition_code][location['zipcode']] += 1
    
    logger.info(f"[OK] Processed {len(patient_conditions)} patient-condition relationships")
    
    # OUTPUT: Partition by CONDITION then DATE (optimal for big data + zipcode aggregation)
    logger.info("")
    logger.info("Generating partitioned gold layer (condition/date.jsonl with H3 binning)...")
    logger.info("")
    logger.info("H3 Configuration:")
    logger.info("  Resolution: 10 (approx 1.7 km hexagons)")
    logger.info("  Aggregation: One record per H3 cell (hexagon center) per date per condition")
    logger.info("")
    
    # H3 cell resolution (10 = ~1.7 km, good for city-level detail)
    H3_RESOLUTION = 10
    
    # Group data by (condition, date, h3_id) for aggregation
    partitioned_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        'city': None,
        'state': None,
        'case_count': 0
    })))
    
    for condition, locations_dict in conditions_by_location.items():
        for location_name, loc_data in locations_dict.items():
            # Skip locations without coordinates
            if loc_data['latitude'] is None or loc_data['longitude'] is None:
                continue
            
            # Convert lat/long to H3 cell ID
            try:
                h3_id = h3.latlng_to_cell(loc_data['latitude'], loc_data['longitude'], H3_RESOLUTION)
            except:
                continue
            
            # Process each date occurrence
            for date_str in loc_data['dates']:
                try:
                    date_obj = datetime.fromisoformat(date_str)
                    date_key = date_obj.strftime("%Y-%m-%d")
                    
                    # Aggregate at H3 cell level
                    agg = partitioned_data[condition][date_key][h3_id]
                    agg['city'] = loc_data['city']
                    agg['state'] = loc_data['state']
                    agg['case_count'] += 1
                except:
                    pass
    
    # Write partitioned output: gold/condition/date.jsonl
    total_partitions = 0
    h3_metadata = {}  # h3_id -> {city, state, latitude (hexagon center), longitude (hexagon center)}
    
    for condition in sorted(partitioned_data.keys()):
        # Sanitize condition name for directory
        condition_dir_name = condition.replace(" (", "_").replace(")", "").replace("/", "_").replace(" ", "_")
        condition_dir = gold_path / condition_dir_name
        condition_dir.mkdir(parents=True, exist_ok=True)
        
        for date_key in sorted(partitioned_data[condition].keys()):
            # Write JSONL: one record per H3 cell per date (normalized: h3 + case_count only)
            output_file = condition_dir / f"{date_key}.jsonl"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for h3_id, agg_data in partitioned_data[condition][date_key].items():
                    # Minimal record: h3 ID (hexagon) + case count
                    record = {
                        "date": date_key,
                        "h3": h3_id,
                        "case_count": agg_data['case_count']
                    }
                    f.write(json.dumps(record) + '\n')
                    
                    # Collect H3 cell metadata (hexagon center coordinates)
                    if h3_id not in h3_metadata:
                        try:
                            # Get hexagon center coordinates
                            center_lat, center_lng = h3.cell_to_latlng(h3_id)
                            h3_metadata[h3_id] = {
                                'city': agg_data['city'],
                                'state': agg_data['state'],
                                'latitude': center_lat,
                                'longitude': center_lng
                            }
                        except:
                            pass
            
            total_partitions += 1
    
    logger.info(f"[OK] Created {total_partitions} partitions (condition/date.jsonl with H3 binning)")
    logger.info("")
    logger.info("Partition structure (H3 hexagonal binning - normalized):")
    logger.info("  gold/")
    logger.info("  ├── Acute_bronchitis/")
    logger.info("  │   ├── 2026-03-22.jsonl  (one record per H3 cell)")
    logger.info("  │   │   {\"date\":\"2026-03-22\",\"h3\":\"8a...\",\"case_count\":5}")
    logger.info("  │   ├── 2026-03-21.jsonl")
    logger.info("  │   └── ...")
    logger.info("  ├── Flu/")
    logger.info("  │   ├── 2026-03-22.jsonl")
    logger.info("  │   └── ...")
    logger.info("  ├── _conditions.json")
    logger.info("  └── _h3_reference.json  (contains hexagon centers)")
    logger.info("      {\"8a...\":{\"city\":\"Boston\",\"state\":\"MA\",\"latitude\":42.36,\"longitude\":-71.06}}")
    logger.info("")
    logger.info("Query flow:")
    logger.info("  1. Load _h3_reference.json once (hexagon lookup)")
    logger.info("  2. For date range 2026-03-15 to 2026-03-22:")
    logger.info("     Load Flu/2026-03-22.jsonl, Flu/2026-03-21.jsonl, ... (lightweight)")
    logger.info("  3. Join H3 IDs with reference to get coordinates + case counts")
    logger.info("  4. Render hexagons on map")
    logger.info("")
    
    # Metadata: conditions list
    output_conditions = gold_path / "_conditions.json"
    conditions_meta = {
        "generated_at": datetime.now().isoformat(),
        "conditions": sorted(list(all_conditions_set)),
        "total_unique": len(all_conditions_set)
    }
    with open(output_conditions, 'w', encoding='utf-8') as f:
        json.dump(conditions_meta, f, indent=2)
    
    logger.info(f"[OK] Created _conditions.json ({len(all_conditions_set)} conditions)")
    
    # Metadata: H3 cell reference (lat/long lookup - shared across all conditions)
    output_h3_ref = gold_path / "_h3_reference.json"
    with open(output_h3_ref, 'w', encoding='utf-8') as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "h3_resolution": H3_RESOLUTION,
            "h3_cells": h3_metadata,
            "total_unique": len(h3_metadata)
        }, f, indent=2)
    
    logger.info(f"[OK] Created _h3_reference.json ({len(h3_metadata)} H3 cells)")
    
    return 0, "Transformation completed successfully"
