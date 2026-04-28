# Local E2E Pipeline Runner

This folder now includes a configurable end-to-end runner:

- `local_testing/run_e2e_pipeline.py`
- `local_testing/e2e_pipeline_config.json`

## Production Code Enforcement

The runner enforces production-code execution by default:

1. `strict_project_src_modules=true`: every `module` step is resolved and must point to a file under the project `src` tree.
2. `allow_script_steps=false`: wrapper script steps are blocked unless explicitly enabled.

This prevents accidental testing through local wrapper scripts and ensures E2E runs hit actual runtime modules.

## What It Tests

Per-step execution and assertions across layers:

1. Bronze input presence
2. Bronze -> Silver transformation
3. Silver -> Gold transformation
4. Firebase pusher skip/write code paths (configurable)
5. Gold idempotence checks (same input run twice)

The runner enforces production code execution:

- `strict_project_src_modules=true` requires every module target to resolve under `src/`
- `allow_script_steps=false` blocks wrapper/script shortcuts by default

## Quick Start

From project root:

```powershell
& "f:/My Data/Academic/GaTech/IHI/vanguard-health/.venv/Scripts/python.exe" local_testing/run_e2e_pipeline.py --pipeline smoke
```

Idempotence profile:

```powershell
& "f:/My Data/Academic/GaTech/IHI/vanguard-health/.venv/Scripts/python.exe" local_testing/run_e2e_pipeline.py --pipeline gold_idempotence
```

Fetcher-inclusive profile:

```powershell
& "f:/My Data/Academic/GaTech/IHI/vanguard-health/.venv/Scripts/python.exe" local_testing/run_e2e_pipeline.py --pipeline with_fetcher
```

## Configurability

All major inputs/outputs and checks are controlled in JSON:

- `context.paths.bronze/silver/gold`
- `base_env` defaults (Mongo, metadata mode, paths)
- profile-specific steps and env overrides
- per-step checks (`exists`, `glob_min`, `parquet_min_rows`)
- metric capture and equality assertions for idempotence

During preflight you should see lines like:

`[preflight] module target OK: src.spark_jobs.bronze_to_silver -> .../src/spark_jobs/bronze_to_silver.py`

Those lines confirm the pipeline is testing actual production modules.

## Runtime Overrides

Use `--set` for quick test permutations without editing config:

```powershell
& "f:/My Data/Academic/GaTech/IHI/vanguard-health/.venv/Scripts/python.exe" local_testing/run_e2e_pipeline.py --pipeline smoke --set paths.gold="F:/temp/gold_out" --set run_ts="1767225600000"
```

If you intentionally need script steps for a custom scenario, set `allow_script_steps=true` in a custom config.

You can also point to a different config file:

```powershell
& "f:/My Data/Academic/GaTech/IHI/vanguard-health/.venv/Scripts/python.exe" local_testing/run_e2e_pipeline.py --config local_testing/e2e_pipeline_config.json --pipeline smoke
```
