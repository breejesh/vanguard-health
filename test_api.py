#!/usr/bin/env python3
import requests
import json
from collections import Counter

# Test API
url = "http://localhost:3000/api/daily-cells?condition=840539006&startDate=2026-02-20&endDate=2026-02-27"
r = requests.get(url)

print(f"Status: {r.status_code}")
print(f"URL: {url}")

data = r.json()
print(f"\nResponse keys: {list(data.keys())}")

records = data.get("data", [])
print(f"Total records: {len(records)}")

if records:
    h3_counts = Counter(d['h3'] for d in records)
    print(f"Unique H3 cells: {len(h3_counts)}")
    print("\nTop 20 H3 cells by record count:")
    for h3_id, count in h3_counts.most_common(20):
        print(f"  {h3_id}: {count}")
else:
    print("\nNo records returned!")
    print(f"First record (if any): {records[0] if records else 'NONE'}")
