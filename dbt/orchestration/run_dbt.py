from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


TRANSIENT_MARKERS = (
    "connection reset",
    "connection aborted",
    "connection refused",
    "connection timed out",
    "read timed out",
    "service unavailable",
    "temporarily unavailable",
    "temporary failure in name resolution",
    "name resolution error",
    "bad gateway",
    "gateway timeout",
    "http 429",
    "http 500",
    "http 502",
    "http 503",
    "http 504",
    "390100",
    "390111",
)

PERMANENT_MARKERS = (
    "incorrect username or password",
    "authentication",
    "multi-factor",
    "mfa",
    "account identifier",
    "does not exist or not authorized",
    "insufficient privileges",
    "not authorized",
    "database error in model",
    "database error in test",
    "compilation error",
    "failure in test",
    "completed with 1 error",
    "completed with 2 errors",
    "completed with 3 errors",
    "completed with 4 errors",
    "completed with 5 errors",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def redact(text: str, environ: dict[str, str] | None = None) -> str:
    values = environ or os.environ
    for name in (
        "SNOWFLAKE_PASSWORD",
        "KESTRA_PASSWORD",
        "KESTRA_API_TOKEN",
    ):
        secret = values.get(name)
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return text


def classify_failure(output: str) -> str:
    """Return transient only for explicitly recognized connectivity failures."""
    lowered = output.lower()
    if any(marker in lowered for marker in PERMANENT_MARKERS):
        return "permanent"
    if any(marker in lowered for marker in TRANSIENT_MARKERS):
        return "transient"
    return "permanent"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def copy_if_present(source: Path, destination: Path) -> dict | None:
    if not source.is_file():
        return None
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return {
        "path": destination.as_posix(),
        "size_bytes": destination.stat().st_size,
        "sha256": sha256_file(destination),
    }


def run_build(
    *,
    project_dir: Path,
    artifacts_dir: Path,
    target: str,
    manifest: dict,
    max_attempts: int = 3,
    retry_base_seconds: int = 10,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    attempts: list[dict] = []
    final_exit_code = 1
    final_classification = "permanent"
    copied: dict[str, dict] = {}

    dbt_manifest = {
        asset: {
            "ingestion_run_id": details["ingestion_run_id"],
            "source_file_sha256": details["source_file_sha256"],
        }
        for asset, details in sorted(manifest.items())
    }
    vars_json = json.dumps(
        {"transfermarkt_source_manifest": dbt_manifest},
        sort_keys=True,
        separators=(",", ":"),
    )

    for attempt in range(1, max_attempts + 1):
        attempt_root = artifacts_dir / f"attempt-{attempt}"
        target_path = attempt_root / "target"
        log_path = attempt_root / "logs"
        target_path.mkdir(parents=True, exist_ok=True)
        log_path.mkdir(parents=True, exist_ok=True)
        console_path = attempt_root / "dbt_console.log"

        env = os.environ.copy()
        env.update(
            {
                "DBT_TARGET": target,
                "DBT_PROFILES_DIR": str(project_dir),
                "DBT_TARGET_PATH": str(target_path),
                "DBT_LOG_PATH": str(log_path),
            }
        )
        command = [
            "dbt",
            "build",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
            "--target",
            target,
            "--select",
            "+tag:transfermarkt_silver",
            "--vars",
            vars_json,
            "--no-version-check",
            "--no-partial-parse",
        ]
        started_at = utc_now()
        collected: list[str] = []
        try:
            process = subprocess.Popen(
                command,
                cwd=project_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            assert process.stdout is not None
            with console_path.open("w", encoding="utf-8", newline="\n") as log:
                for line in process.stdout:
                    safe = redact(line, env)
                    collected.append(safe)
                    log.write(safe)
                    print(safe, end="")
            final_exit_code = int(process.wait())
        except Exception as exc:  # Preserve evidence for the validation task.
            safe = redact(f"{type(exc).__name__}: {exc}", env)
            collected.append(safe)
            console_path.write_text(safe + "\n", encoding="utf-8")
            print(safe, file=sys.stderr)
            final_exit_code = 1

        combined = "".join(collected)
        final_classification = (
            "success" if final_exit_code == 0 else classify_failure(combined)
        )
        attempts.append(
            {
                "attempt": attempt,
                "started_at": started_at,
                "finished_at": utc_now(),
                "exit_code": final_exit_code,
                "classification": final_classification,
                "console_log": console_path.relative_to(artifacts_dir).as_posix(),
            }
        )

        for name in ("manifest.json", "run_results.json"):
            metadata = copy_if_present(target_path / name, artifacts_dir / name)
            if metadata:
                copied[name] = metadata

        if final_exit_code == 0:
            break
        if final_classification != "transient" or attempt == max_attempts:
            break
        sleeper(min(retry_base_seconds * (2 ** (attempt - 1)), 120))

    result = {
        "status": "SUCCESS" if final_exit_code == 0 else "FAILED",
        "dbt_exit_code": final_exit_code,
        "attempt_count": len(attempts),
        "failure_classification": final_classification,
        "selection": "+tag:transfermarkt_silver",
        "target": target,
        "attempts": attempts,
        "dbt_artifacts": copied,
        "finished_at": utc_now(),
    }
    (artifacts_dir / "dbt_execution.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Run the fixed-manifest Silver dbt build")
    root.add_argument("--project-dir", type=Path, required=True)
    root.add_argument("--plan", type=Path, required=True)
    root.add_argument("--artifacts-dir", type=Path, required=True)
    root.add_argument("--target", choices=("dev", "test", "prod"), required=True)
    root.add_argument("--max-attempts", type=int, default=3)
    root.add_argument(
        "--retry-base-seconds",
        type=int,
        default=int(os.getenv("SILVER_RETRY_BASE_SECONDS", "10")),
    )
    return root


def main(argv: Iterable[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not 1 <= args.max_attempts <= 3:
        raise ValueError("max-attempts must be between 1 and 3")
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    result = run_build(
        project_dir=args.project_dir.resolve(),
        artifacts_dir=args.artifacts_dir.resolve(),
        target=args.target,
        manifest=plan["manifest"],
        max_attempts=args.max_attempts,
        retry_base_seconds=max(0, args.retry_base_seconds),
    )
    # Return zero so Kestra persists artifacts. The following validation task
    # turns a failed dbt result into a failed flow and never advances checkpoint.
    print(
        "::"
        + json.dumps(
            {
                "outputs": {
                    "dbt_status": result["status"],
                    "dbt_exit_code": result["dbt_exit_code"],
                    "attempt_count": result["attempt_count"],
                }
            },
            separators=(",", ":"),
        )
        + "::"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(redact(f"ERROR {type(exc).__name__}: {exc}"), file=sys.stderr)
        raise SystemExit(1)
