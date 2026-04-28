#!/usr/bin/env python
"""
Configurable local E2E pipeline runner for Vanguard Health.

This runner executes production modules step-by-step and validates outputs per layer.
Use JSON config profiles to quickly switch inputs/outputs and cover different code paths.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.compute as pc
import pyarrow.parquet as pq


_TEMPLATE_PATTERN = re.compile(r"\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}")


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def _parse_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _set_dotted(target: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    current: Dict[str, Any] = target
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _get_dotted(source: Dict[str, Any], dotted_key: str) -> Any:
    current: Any = source
    for part in dotted_key.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(f"Missing template key: {dotted_key}")
        current = current[part]
    return current


def _resolve_templates(value: Any, context: Dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {k: _resolve_templates(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_templates(v, context) for v in value]
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            key = match.group(1)
            return str(_get_dotted(context, key))

        return _TEMPLATE_PATTERN.sub(_replace, value)
    return value


def _resolve_context_templates(context: Dict[str, Any], max_passes: int = 5) -> Dict[str, Any]:
    """Resolve nested template references inside context itself."""
    resolved = json.loads(json.dumps(context))
    for _ in range(max_passes):
        next_resolved = _resolve_templates(resolved, resolved)
        if next_resolved == resolved:
            break
        resolved = next_resolved
    return resolved


def _build_windows_spark_env(project_root: Path, env: Dict[str, str]) -> Dict[str, str]:
    if sys.platform != "win32":
        return env

    local_testing = project_root / "local_testing"
    hadoop_home = str(local_testing).replace("\\", "/")
    bin_dir = local_testing / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    out = dict(env)
    out.setdefault("PYTHONIOENCODING", "utf-8")
    out.setdefault("PYTHONUTF8", "1")
    out.setdefault("HADOOP_HOME", hadoop_home)
    out.setdefault("hadoop.home.dir", hadoop_home)
    out.setdefault("JAVA_TOOL_OPTIONS", f'-Dhadoop.home.dir="{hadoop_home}"')

    venv_site_packages = project_root / ".venv" / "Lib" / "site-packages"
    if venv_site_packages.exists():
        previous = out.get("PYTHONPATH", "")
        out["PYTHONPATH"] = f"{venv_site_packages};{previous}" if previous else str(venv_site_packages)

    return out


def _find_files(base_path: Path, pattern: str, exclude_prefix: str | None = None) -> List[Path]:
    if not base_path.exists():
        return []
    matches = [p for p in base_path.glob(pattern) if p.is_file()]
    if exclude_prefix:
        matches = [p for p in matches if not p.name.startswith(exclude_prefix)]
    return sorted(matches)


def _capture_parquet_stats(base_path: Path, pattern: str, exclude_prefix: str | None, sum_columns: List[str]) -> Dict[str, Any]:
    files = _find_files(base_path, pattern, exclude_prefix=exclude_prefix)
    row_count = 0
    sums: Dict[str, float] = {col: 0.0 for col in sum_columns}

    for parquet_file in files:
        parquet_obj = pq.ParquetFile(str(parquet_file))
        row_count += int(parquet_obj.metadata.num_rows)

        for column_name in sum_columns:
            table = pq.read_table(str(parquet_file), columns=[column_name])
            value = pc.sum(table[column_name]).as_py()
            if value is not None:
                sums[column_name] += float(value)

    return {
        "file_count": len(files),
        "row_count": row_count,
        "sums": sums,
    }


def _run_command(command: List[str], cwd: Path, env: Dict[str, str]) -> None:
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=str(cwd), env=env, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {' '.join(command)}")


def _resolve_module_source(module_name: str) -> Path:
    spec = importlib.util.find_spec(module_name)
    if spec is None:
        raise RuntimeError(f"Cannot resolve module spec for: {module_name}")
    if not spec.origin:
        raise RuntimeError(f"Module has no concrete source file: {module_name}")
    return Path(spec.origin).resolve()


def _assert_module_is_project_src(module_name: str, project_root: Path) -> Path:
    module_path = _resolve_module_source(module_name)
    src_root = (project_root / "src").resolve()

    if src_root not in module_path.parents:
        raise RuntimeError(
            f"Module '{module_name}' resolves to non-src path: {module_path}. "
            f"Expected a production module under {src_root}"
        )

    return module_path


def _run_checks(step_name: str, checks: List[Dict[str, Any]], context: Dict[str, Any], metrics: Dict[str, Any]) -> None:
    for check in checks:
        resolved = _resolve_templates(check, context)
        check_type = resolved.get("type")

        if check_type == "exists":
            path = Path(resolved["path"])
            if not path.exists():
                raise AssertionError(f"[{step_name}] Path does not exist: {path}")
            print(f"[{step_name}] check exists OK: {path}")

        elif check_type == "glob_min":
            base_path = Path(resolved["path"])
            pattern = str(resolved["pattern"])
            min_count = int(resolved.get("min", 1))
            exclude_prefix = resolved.get("exclude_prefix")
            matches = _find_files(base_path, pattern, exclude_prefix=exclude_prefix)
            if len(matches) < min_count:
                raise AssertionError(
                    f"[{step_name}] Expected at least {min_count} files in {base_path} matching {pattern}, found {len(matches)}"
                )
            print(f"[{step_name}] check glob_min OK: {len(matches)} files")

        elif check_type == "parquet_min_rows":
            base_path = Path(resolved["path"])
            pattern = str(resolved["pattern"])
            min_rows = int(resolved.get("min_rows", 1))
            exclude_prefix = resolved.get("exclude_prefix")
            stats = _capture_parquet_stats(base_path, pattern, exclude_prefix, sum_columns=[])
            if stats["row_count"] < min_rows:
                raise AssertionError(
                    f"[{step_name}] Expected at least {min_rows} parquet rows in {base_path} matching {pattern}, got {stats['row_count']}"
                )
            print(f"[{step_name}] check parquet_min_rows OK: {stats['row_count']} rows")

        elif check_type == "capture_parquet_stats":
            metric_id = str(resolved["id"])
            base_path = Path(resolved["path"])
            pattern = str(resolved["pattern"])
            exclude_prefix = resolved.get("exclude_prefix")
            sum_columns = [str(col) for col in resolved.get("sum_columns", [])]
            metrics[metric_id] = _capture_parquet_stats(base_path, pattern, exclude_prefix, sum_columns)
            print(f"[{step_name}] captured metric {metric_id}: {metrics[metric_id]}")

        elif check_type == "assert_metric_equal":
            left_key = str(resolved["left"])
            right_key = str(resolved["right"])
            if left_key not in metrics or right_key not in metrics:
                raise AssertionError(f"[{step_name}] Missing metric(s) for comparison: {left_key}, {right_key}")
            if metrics[left_key] != metrics[right_key]:
                raise AssertionError(
                    f"[{step_name}] Metrics differ: {left_key}={metrics[left_key]} vs {right_key}={metrics[right_key]}"
                )
            print(f"[{step_name}] check assert_metric_equal OK: {left_key} == {right_key}")

        else:
            raise ValueError(f"[{step_name}] Unsupported check type: {check_type}")


def run_pipeline(config: Dict[str, Any], pipeline_name: str, context_overrides: Dict[str, Any]) -> None:
    project_root = Path(__file__).resolve().parent.parent

    # Ensure preflight module resolution works even when launched from local_testing/.
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    context: Dict[str, Any] = config.get("context", {})
    context = json.loads(json.dumps(context))
    for dotted_key, value in context_overrides.items():
        _set_dotted(context, dotted_key, value)

    context.setdefault("project_root", str(project_root))
    context.setdefault("run_ts", str(int(time.time() * 1000)))
    context.setdefault("pipeline_run_id", str(uuid.uuid4()))
    context = _resolve_context_templates(context)

    pipelines = config.get("pipelines", {})
    if pipeline_name not in pipelines:
        raise KeyError(f"Unknown pipeline '{pipeline_name}'. Available: {', '.join(sorted(pipelines.keys()))}")

    pipeline = pipelines[pipeline_name]
    steps = pipeline.get("steps", [])
    if not steps:
        raise ValueError(f"Pipeline '{pipeline_name}' has no steps")

    strict_project_src_modules = bool(config.get("strict_project_src_modules", True))
    allow_script_steps = bool(config.get("allow_script_steps", False))

    print("=" * 80)
    print(f"Pipeline: {pipeline_name}")
    print(pipeline.get("description", ""))
    print(f"run_ts={context['run_ts']}")
    print(f"pipeline_run_id={context['pipeline_run_id']}")
    print(f"strict_project_src_modules={strict_project_src_modules}")
    print(f"allow_script_steps={allow_script_steps}")
    print("=" * 80)

    # Preflight: ensure module targets resolve to production src files.
    for raw_step in steps:
        step = _resolve_templates(raw_step, context)
        if not step.get("enabled", True):
            continue

        step_type = str(step.get("type", "module"))
        if step_type == "module":
            module_name = str(step["target"])
            module_path = _resolve_module_source(module_name)
            if strict_project_src_modules:
                module_path = _assert_module_is_project_src(module_name, project_root)
            print(f"[preflight] module target OK: {module_name} -> {module_path}")
        elif step_type == "script" and not allow_script_steps:
            raise RuntimeError(
                "Script steps are disabled by configuration (allow_script_steps=false). "
                f"Step target: {step.get('target')}"
            )

    base_env = {k: str(v) for k, v in os.environ.items()}
    base_env_updates = _resolve_templates(config.get("base_env", {}), context)
    for k, v in base_env_updates.items():
        base_env[str(k)] = str(v)

    base_env = _build_windows_spark_env(project_root, base_env)
    metrics: Dict[str, Any] = {}

    for index, raw_step in enumerate(steps, start=1):
        step = _resolve_templates(raw_step, context)
        if not step.get("enabled", True):
            print(f"[{index}/{len(steps)}] Skipping disabled step: {step.get('name', 'unnamed')}")
            continue

        step_name = str(step.get("name", f"step_{index}"))
        step_type = str(step.get("type", "module"))
        print(f"\n[{index}/{len(steps)}] {step_name} ({step_type})")

        step_env = dict(base_env)
        for k, v in step.get("env", {}).items():
            step_env[str(k)] = str(v)

        if step_type == "module":
            module_name = str(step["target"])
            command = [sys.executable, "-m", module_name]
            command.extend([str(arg) for arg in step.get("args", [])])
            _run_command(command, cwd=project_root, env=step_env)

        elif step_type == "script":
            if not allow_script_steps:
                raise RuntimeError(
                    "Script steps are disabled by configuration (allow_script_steps=false). "
                    f"Step target: {step.get('target')}"
                )
            script_path = str(step["target"])
            command = [sys.executable, script_path]
            command.extend([str(arg) for arg in step.get("args", [])])
            _run_command(command, cwd=project_root, env=step_env)

        elif step_type == "check":
            pass

        else:
            raise ValueError(f"Unsupported step type: {step_type}")

        _run_checks(step_name, step.get("checks", []), context, metrics)

    print("\n" + "=" * 80)
    print(f"Pipeline '{pipeline_name}' completed successfully")
    print("=" * 80)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run configurable local E2E pipeline")
    parser.add_argument(
        "--config",
        default="local_testing/e2e_pipeline_config.json",
        help="Path to pipeline config JSON",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Pipeline profile name (defaults to config.default_pipeline)",
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Context override, for example --set paths.gold=local_testing/data/gold_test",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path(__file__).resolve().parent.parent / config_path

    if not config_path.exists():
        print(f"Config not found: {config_path}")
        return 2

    config = _load_json(config_path)
    pipeline_name = args.pipeline or config.get("default_pipeline")
    if not pipeline_name:
        print("No pipeline selected. Set --pipeline or config.default_pipeline")
        return 2

    overrides: Dict[str, Any] = {}
    for item in args.set:
        if "=" not in item:
            print(f"Invalid --set argument: {item}")
            return 2
        key, raw_value = item.split("=", 1)
        overrides[key] = _parse_value(raw_value)

    try:
        run_pipeline(config, pipeline_name, overrides)
        return 0
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
