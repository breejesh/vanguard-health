#!/usr/bin/env python3
"""Remove old code from generate_gold_from_real_data.py"""

with open('scripts/generate_gold_from_real_data.py', 'r') as f:
    lines = f.readlines()

# Keep only lines up to the first if __name__ + main() call (line 239)
# This removes all the old duplicate code
cleaned_lines = lines[:239]

with open('scripts/generate_gold_from_real_data.py', 'w') as f:
    f.writelines(cleaned_lines)

print(f"✓ Cleaned file: kept {len(cleaned_lines)} lines, removed {len(lines) - len(cleaned_lines)}")
