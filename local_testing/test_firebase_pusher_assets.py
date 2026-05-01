#!/usr/bin/env python
"""
Run the actual Firebase pusher against real local gold data.

This script invokes src/common/firebase_pusher.py directly and pushes
local_testing/data/gold content to Firebase Hosting via REST API.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent


def _load_project_id_from_credentials(credentials_path: Path) -> str | None:
    if not credentials_path.exists() or not credentials_path.is_file():
        return None
    try:
        payload = json.loads(credentials_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    project_id = payload.get("project_id")
    return str(project_id) if project_id else None


def run_actual_push() -> int:
    source_gold = project_root / "local_testing" / "data" / "gold"
    if not source_gold.exists():
        print(f"Missing source gold directory: {source_gold}")
        return 2

    env = dict(os.environ)
    env.setdefault("GOLD_PATH", str(source_gold))
    env.setdefault("WEBSITE_GOLD_PATH", str(source_gold))
    env["ENABLE_FIREBASE_SYNC"] = "true"
    env["PUSH_TO_FIREBASE"] = "true"

    default_creds_path = project_root / "vanguard-health-firebase-adminsdk-fbsvc-d3d5fbe79f.json"
    env.setdefault("FIREBASE_CREDENTIALS_PATH", str(default_creds_path))

    if not env.get("FIREBASE_PROJECT_ID"):
        loaded_project_id = _load_project_id_from_credentials(Path(env["FIREBASE_CREDENTIALS_PATH"]))
        if loaded_project_id:
            env["FIREBASE_PROJECT_ID"] = loaded_project_id

    if not env.get("FIREBASE_HOSTING_SITE_ID") and env.get("FIREBASE_PROJECT_ID"):
        env["FIREBASE_HOSTING_SITE_ID"] = env["FIREBASE_PROJECT_ID"]

    if not env.get("FIREBASE_HOSTING_SITE_ID") and not env.get("FIREBASE_PROJECT_ID"):
        print("Missing FIREBASE_HOSTING_SITE_ID/FIREBASE_PROJECT_ID in environment")
        return 2

    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{project_root}{os.pathsep}{existing_pythonpath}"
        if existing_pythonpath
        else str(project_root)
    )

    script_path = project_root / "src" / "common" / "firebase_pusher.py"
    print("Running actual Firebase pusher with:")
    print(f"  GOLD_PATH={env['GOLD_PATH']}")
    print(f"  WEBSITE_GOLD_PATH={env['WEBSITE_GOLD_PATH']}")
    print(f"  FIREBASE_HOSTING_SITE_ID={env.get('FIREBASE_HOSTING_SITE_ID', '')}")
    print(f"  FIREBASE_PROJECT_ID={env.get('FIREBASE_PROJECT_ID', '')}")
    print(f"  FIREBASE_CREDENTIALS_PATH={env.get('FIREBASE_CREDENTIALS_PATH', '')}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        env=env,
        check=False,
    )
    if result.returncode != 0:
        print(f"firebase_pusher.py failed with exit code {result.returncode}")
        return result.returncode

    print("firebase_pusher.py completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_actual_push())
