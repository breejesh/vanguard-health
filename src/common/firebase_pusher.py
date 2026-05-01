"""
Refresh website gold assets from parquet outputs.

This runs as a standalone pipeline stage after Silver->Gold completes.
It copies parquet artifacts from GOLD_PATH into the frontend asset source
directory that is later shipped as /assets/gold.
"""
import sys
import os
import shutil
import gzip
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from src.common.logger import setup_logger
from src.common.config import get_config

logger = setup_logger(__name__)
config = get_config()

HOSTING_API_BASE = "https://firebasehosting.googleapis.com/v1beta1"


def _resolve_assets_gold_path() -> Path:
    """
    Resolve the folder that feeds hosted website assets/gold.

    Angular build currently maps `local_testing/data/gold` to `/assets/gold`.
    WEBSITE_GOLD_PATH can override this when needed.
    """
    env_target = os.getenv("WEBSITE_GOLD_PATH")
    if env_target:
        return Path(env_target)

    return Path("local_testing") / "data" / "gold"


def _list_parquet_relative_paths(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(
        [p.relative_to(root) for p in root.rglob("*.parquet") if p.is_file()]
    )


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _resolve_credentials_info() -> Dict[str, str]:
    raw = os.getenv("FIREBASE_CREDENTIALS_JSON") or os.getenv("FIREBASE_CREDENTIALS")
    if raw:
        raw = raw.strip()
        if raw.startswith("{"):
            return json.loads(raw)
        maybe_path = Path(raw)
        if maybe_path.exists() and maybe_path.is_file():
            return json.loads(maybe_path.read_text(encoding="utf-8"))

    env_path = os.getenv("FIREBASE_CREDENTIALS_PATH")
    if env_path:
        p = Path(env_path)
        if p.exists() and p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))

    default_path = Path(config.FIREBASE_CREDENTIALS_PATH)
    if default_path.exists() and default_path.is_file():
        return json.loads(default_path.read_text(encoding="utf-8"))

    raise FileNotFoundError(
        "Firebase credentials not found. Set FIREBASE_CREDENTIALS_JSON, FIREBASE_CREDENTIALS, or FIREBASE_CREDENTIALS_PATH."
    )


def _get_access_token() -> str:
    creds_info = _resolve_credentials_info()
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/firebase"],
    )
    creds.refresh(Request())
    if not creds.token:
        raise RuntimeError("Failed to obtain Firebase access token")
    return str(creds.token)


def _gzip_and_hash(file_path: Path) -> Tuple[bytes, str]:
    raw = file_path.read_bytes()
    gz = gzip.compress(raw)
    file_hash = hashlib.sha256(gz).hexdigest()
    return gz, file_hash


def _get_latest_release_version_name(site_id: str, headers: Dict[str, str]) -> str | None:
    resp = requests.get(
        f"{HOSTING_API_BASE}/sites/{site_id}/releases",
        headers=headers,
        params={"pageSize": 1},
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    releases = payload.get("releases", [])
    if not releases:
        return None
    latest = releases[0]
    version_obj = latest.get("version", {})
    version_name = version_obj.get("name")
    return str(version_name) if version_name else None


def _list_version_files(version_name: str, headers: Dict[str, str]) -> Dict[str, str]:
    file_hashes: Dict[str, str] = {}
    next_page_token = ""

    while True:
        params: Dict[str, str | int] = {"pageSize": 1000}
        if next_page_token:
            params["pageToken"] = next_page_token

        resp = requests.get(
            f"{HOSTING_API_BASE}/{version_name}/files",
            headers=headers,
            params=params,
            timeout=120,
        )
        resp.raise_for_status()
        payload = resp.json()

        for item in payload.get("files", []):
            path = item.get("path")
            file_hash = item.get("hash")
            if path and file_hash:
                file_hashes[str(path)] = str(file_hash)

        next_page_token = str(payload.get("nextPageToken") or "")
        if not next_page_token:
            break

    return file_hashes


def _get_version_config(version_name: str, headers: Dict[str, str]) -> Dict:
    resp = requests.get(
        f"{HOSTING_API_BASE}/{version_name}",
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    config_obj = payload.get("config")
    return config_obj if isinstance(config_obj, dict) else {}


def sync_gold_parquet_assets(source_gold_dir: Path, website_gold_dir: Path) -> Tuple[int, int, int]:
    """Sync parquet files from GOLD_PATH into website asset source path."""
    if not source_gold_dir.exists():
        logger.warning(f"Gold source path does not exist: {source_gold_dir}")
        return 0, 0, 0

    website_gold_dir.mkdir(parents=True, exist_ok=True)

    source_rel_paths = _list_parquet_relative_paths(source_gold_dir)
    source_rel_set: Set[Path] = set(source_rel_paths)
    target_rel_set: Set[Path] = set(_list_parquet_relative_paths(website_gold_dir))

    copied = 0
    removed = 0
    skipped = 0

    for rel_path in source_rel_paths:
        src_file = source_gold_dir / rel_path
        dst_file = website_gold_dir / rel_path
        _ensure_parent_dir(dst_file)

        # Skip unchanged parquet files to refresh only necessary assets.
        if dst_file.exists():
            src_stat = src_file.stat()
            dst_stat = dst_file.stat()
            if src_stat.st_size == dst_stat.st_size and int(src_stat.st_mtime) == int(dst_stat.st_mtime):
                skipped += 1
                continue

        shutil.copy2(src_file, dst_file)
        copied += 1

    for stale_rel_path in sorted(target_rel_set - source_rel_set):
        stale_file = website_gold_dir / stale_rel_path
        if stale_file.exists():
            stale_file.unlink()
            removed += 1

    # Remove legacy json/jsonl files that should never be in gold assets.
    for legacy in website_gold_dir.rglob("*.json"):
        legacy.unlink()
        removed += 1
    for legacy in website_gold_dir.rglob("*.jsonl"):
        legacy.unlink()
        removed += 1

    # Remove empty directories created by stale deletions.
    for subdir in sorted([p for p in website_gold_dir.rglob("*") if p.is_dir()], key=lambda p: len(p.parts), reverse=True):
        try:
            subdir.rmdir()
        except OSError:
            continue

    return copied, removed, skipped


def deploy_hosting_via_rest_api(source_gold_dir: Path) -> None:
    """Deploy /assets/gold parquet files directly from container using Hosting REST API."""
    site_id = os.getenv("FIREBASE_HOSTING_SITE_ID", config.FIREBASE_PROJECT_ID)
    if not site_id:
        raise RuntimeError("Missing FIREBASE_HOSTING_SITE_ID and FIREBASE_PROJECT_ID")

    parquet_files = [p for p in source_gold_dir.rglob("*.parquet") if p.is_file()]
    if not parquet_files:
        logger.warning(f"No parquet files found to deploy from {source_gold_dir}")
        return

    token = _get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Start from currently live release to preserve all non-gold website files.
    live_version_name = _get_latest_release_version_name(site_id, headers)
    file_payload: Dict[str, str] = {}
    create_body: Dict[str, Dict] = {}
    if live_version_name:
        live_files = _list_version_files(live_version_name, headers)
        live_config = _get_version_config(live_version_name, headers)
        if live_config:
            create_body["config"] = live_config
        # Preserve every path except assets/gold, which this job manages.
        file_payload = {
            path: file_hash
            for path, file_hash in live_files.items()
            if not str(path).startswith("/assets/gold/")
        }
        logger.info(
            f"Loaded {len(live_files)} files from live version {live_version_name}; "
            f"preserving {len(file_payload)} non-gold paths"
        )
    else:
        strict_selective = os.getenv("STRICT_SELECTIVE_DEPLOY", "true").lower() == "true"
        if strict_selective:
            raise RuntimeError(
                "No existing Hosting release found to preserve non-gold files. "
                "Run one full website deploy first (or set STRICT_SELECTIVE_DEPLOY=false to allow initial gold-only deploy)."
            )
        logger.warning("No existing release found; deploying assets/gold as initial version content")

    gz_payload_by_hash: Dict[str, bytes] = {}

    for file_path in parquet_files:
        rel = file_path.relative_to(source_gold_dir).as_posix()
        hosting_path = f"/assets/gold/{rel}"
        gz_bytes, file_hash = _gzip_and_hash(file_path)
        file_payload[hosting_path] = file_hash
        gz_payload_by_hash[file_hash] = gz_bytes

    create_resp = requests.post(
        f"{HOSTING_API_BASE}/sites/{site_id}/versions",
        headers=headers,
        json=create_body,
        timeout=60,
    )
    create_resp.raise_for_status()
    version_name = create_resp.json().get("name")
    if not version_name:
        raise RuntimeError(f"Unexpected versions.create response: {create_resp.text}")

    populate_resp = requests.post(
        f"{HOSTING_API_BASE}/{version_name}:populateFiles",
        headers=headers,
        json={"files": file_payload},
        timeout=120,
    )
    populate_resp.raise_for_status()
    populate_body = populate_resp.json()
    upload_url = populate_body.get("uploadUrl")
    required_hashes = set(populate_body.get("uploadRequiredHashes", []))
    if not upload_url:
        raise RuntimeError(f"Missing uploadUrl in populateFiles response: {populate_resp.text}")

    upload_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"}
    for required_hash in required_hashes:
        gz_bytes = gz_payload_by_hash.get(required_hash)
        if gz_bytes is None:
            raise RuntimeError(f"populateFiles requested unknown hash: {required_hash}")
        upload_resp = requests.post(
            f"{upload_url}/{required_hash}",
            headers=upload_headers,
            data=gz_bytes,
            timeout=180,
        )
        upload_resp.raise_for_status()

    patch_resp = requests.patch(
        f"{HOSTING_API_BASE}/{version_name}?update_mask=status",
        headers=headers,
        json={"status": "FINALIZED"},
        timeout=60,
    )
    patch_resp.raise_for_status()

    release_resp = requests.post(
        f"{HOSTING_API_BASE}/sites/{site_id}/releases",
        headers={"Authorization": f"Bearer {token}"},
        params={"versionName": version_name},
        timeout=60,
    )
    release_resp.raise_for_status()

    logger.info(
        f"Firebase Hosting REST deploy complete for site={site_id}; "
        f"files={len(file_payload)}, uploaded={len(required_hashes)}"
    )


def deploy_hosting(source_gold_dir: Path) -> None:
    """Deploy refreshed gold assets to Firebase Hosting via REST API only."""
    push_enabled = os.getenv("PUSH_TO_FIREBASE", "true").lower() == "true"
    if not push_enabled:
        logger.info("PUSH_TO_FIREBASE=false: skipping Firebase Hosting deploy")
        return
    deploy_hosting_via_rest_api(source_gold_dir)


def main():
    try:
        logger.info("Starting gold asset refresh stage")

        # Keep backward compatibility with existing config/env flag.
        # If disabled, no sync is performed.
        if not config.ENABLE_FIREBASE_SYNC:
            logger.info("ENABLE_FIREBASE_SYNC=false: skipping website gold asset refresh")
            return 0

        source_gold_dir = Path(os.environ.get("GOLD_PATH", "/mnt/data/gold"))
        website_gold_dir = _resolve_assets_gold_path()

        copied, removed, skipped = sync_gold_parquet_assets(source_gold_dir, website_gold_dir)

        logger.info(
            "Gold asset refresh complete: "
            f"copied={copied}, removed={removed}, skipped={skipped}, "
            f"target={website_gold_dir}"
        )

        deploy_hosting(source_gold_dir)
        logger.info("Firebase Hosting deploy completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Gold asset refresh failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
