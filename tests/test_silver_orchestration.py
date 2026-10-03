from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
import yaml

from ingestion.silver import (
    FINAL_MODELS,
    REQUIRED_ASSETS,
    SilverControl,
    archive_project,
    command_plan,
    decide_run,
    manifest_fingerprint,
    parse_fixed_manifest,
    parser,
    transformation_fingerprint,
    validate_dbt_artifacts,
    is_processing_column,
)


ROOT = Path(__file__).parents[1]


def _load_dbt_runner():
    path = ROOT / "dbt/orchestration/run_dbt.py"
    spec = importlib.util.spec_from_file_location("transfermarkt_dbt_runner", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _manifest() -> dict:
    return {
        asset: {
            "ingestion_run_id": f"run-{asset}",
            "source_file_sha256": (f"{index:064x}"),
        }
        for index, asset in enumerate(REQUIRED_ASSETS, start=1)
    }


def test_content_fingerprint_detects_local_change_and_archive_is_stable(tmp_path):
    project = tmp_path / "dbt"
    (project / "models").mkdir(parents=True)
    (project / "orchestration").mkdir()
    (project / "dbt_project.yml").write_text("name: fixture\n", encoding="utf-8")
    model = project / "models/example.sql"
    model.write_text("select 1 as value\n", encoding="utf-8")
    (project / "orchestration/run_dbt.py").write_text("print('runner')\n", encoding="utf-8")

    first = transformation_fingerprint(project)
    archive_one = tmp_path / "one.tar.gz"
    archive_two = tmp_path / "two.tar.gz"
    archive_project(project, archive_one)
    archive_project(project, archive_two)

    model.write_text("select 2 as value\n", encoding="utf-8")
    second = transformation_fingerprint(project)

    assert first != second
    assert archive_one.read_bytes() == archive_two.read_bytes()


def test_repeatability_signature_excludes_processing_and_lineage_metadata():
    assert is_processing_column("silver_processed_at")
    assert is_processing_column("players_current_processed_at")
    assert is_processing_column("manifest_resolved_at")
    assert is_processing_column("players_source_captured_at")
    assert is_processing_column("players_source_version")
    assert is_processing_column("players_source_version_checksum")
    assert is_processing_column("players_bronze_ingestion_run_id")
    assert not is_processing_column("valuation_date")


def test_manifest_requires_every_preserved_source_and_has_stable_identity():
    manifest = _manifest()
    parsed = parse_fixed_manifest(json.dumps(manifest))

    assert parsed == manifest
    assert manifest_fingerprint(parsed) == manifest_fingerprint(dict(reversed(list(parsed.items()))))

    del manifest[REQUIRED_ASSETS[0]]
    with pytest.raises(ValueError, match="missing"):
        parse_fixed_manifest(json.dumps(manifest))


def test_backfill_refuses_to_invent_a_historical_manifest(tmp_path):
    args = parser().parse_args(
        [
            "plan",
            "--project-dir",
            str(tmp_path),
            "--execution-id",
            "fixture",
            "--mode",
            "backfill",
            "--source-manifest",
            "{}",
            "--output",
            str(tmp_path / "plan.json"),
            "--archive",
            str(tmp_path / "project.tar.gz"),
        ]
    )
    with pytest.raises(ValueError, match="requires an explicit preserved"):
        command_plan(args)


def test_decision_covers_success_skip_force_and_failed_run_recovery():
    manifest_hash = "m1"
    transform_hash = "t1"
    checkpoint = {
        "manifest_fingerprint": manifest_hash,
        "transformation_fingerprint": transform_hash,
    }

    should_run, reasons = decide_run(
        force=False,
        missing_tables=[],
        checkpoint=checkpoint,
        current_manifest_fingerprint=manifest_hash,
        current_transformation_fingerprint=transform_hash,
    )
    assert should_run is False
    assert reasons == ["already_validated"]

    assert decide_run(
        force=True,
        missing_tables=[],
        checkpoint=checkpoint,
        current_manifest_fingerprint=manifest_hash,
        current_transformation_fingerprint=transform_hash,
    )[0]

    # A failed run never advances the checkpoint; pending input is detected again.
    should_recover, recovery_reasons = decide_run(
        force=False,
        missing_tables=[],
        checkpoint=checkpoint,
        current_manifest_fingerprint="m2",
        current_transformation_fingerprint=transform_hash,
    )
    assert should_recover
    assert "bronze_versions_pending" in recovery_reasons

    should_rebuild, rebuild_reasons = decide_run(
        force=False,
        missing_tables=[FINAL_MODELS[0]],
        checkpoint=checkpoint,
        current_manifest_fingerprint=manifest_hash,
        current_transformation_fingerprint="t2",
    )
    assert should_rebuild
    assert any(reason.startswith("required_outputs_missing:") for reason in rebuild_reasons)
    assert "transformation_changed" in rebuild_reasons


def test_concurrent_target_is_rejected_without_touching_checkpoint():
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args):
            return self

        def fetchone(self):
            return ("already-running",)

    class Connection:
        def cursor(self):
            return Cursor()

    control = object.__new__(SilverControl)
    control.target = "test"
    control.connection = Connection()
    control.qname = lambda _name: '"DB"."SCHEMA"."RUNS"'

    with pytest.raises(RuntimeError, match="already has active execution"):
        control.assert_no_concurrent_run("new-run")


def test_dbt_validation_accepts_warns_but_rejects_critical_failures():
    success_results = {
        "results": [
            *(
                {
                    "unique_id": f"model.elt_soccer_value.{name}",
                    "status": "success",
                }
                for name in FINAL_MODELS
            ),
            {"unique_id": "test.elt_soccer_value.coverage", "status": "warn"},
        ]
    }
    validated = validate_dbt_artifacts(
        {"status": "SUCCESS", "dbt_exit_code": 0}, success_results
    )
    assert validated["valid"] is True
    assert validated["warnings"] == 1

    failed = validate_dbt_artifacts(
        {"status": "FAILED", "dbt_exit_code": 1, "failure_classification": "permanent"},
        {"results": [{"unique_id": "test.critical", "status": "fail"}]},
    )
    assert failed["valid"] is False
    assert any("critical" in message for message in failed["errors"])


def test_failed_finish_does_not_advance_checkpoint():
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, params=None):
            statements.append((sql, params))
            return self

    class Connection:
        def __init__(self):
            self.commits = 0

        def cursor(self):
            return Cursor()

        def commit(self):
            self.commits += 1

    control = object.__new__(SilverControl)
    control.target = "test"
    control.connection = Connection()
    control.qname = lambda name: f'"DB"."SCHEMA"."{name}"'
    control.finish(
        execution_id="failed-run",
        success=False,
        dbt_execution={"dbt_exit_code": 1, "attempt_count": 1},
        result_summary={"valid": False},
        model_counts={},
        artifact_metadata=[],
        error="critical quality failure",
        plan={"manifest_fingerprint": "m", "transformation_fingerprint": "t", "manifest": {}},
    )

    sql = "\n".join(statement for statement, _params in statements)
    assert "UPDATE" in sql
    assert "TRANSFERMARKT_SILVER_CHECKPOINT" not in sql
    assert control.connection.commits == 1


def test_retry_classifier_only_retries_explicit_transient_failures(monkeypatch):
    runner = _load_dbt_runner()

    assert runner.classify_failure("HTTP 503 service unavailable") == "transient"
    assert runner.classify_failure("Failure in test unique_player_id") == "permanent"
    assert runner.classify_failure("Incorrect username or password; connection timed out") == "permanent"

    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "synthetic-secret")
    assert "synthetic-secret" not in runner.redact("error synthetic-secret")


def test_dbt_runner_caps_transient_attempts_and_never_retries_quality(tmp_path, monkeypatch):
    runner = _load_dbt_runner()
    project = tmp_path / "project"
    project.mkdir()
    manifest = {
        "players": {
            "ingestion_run_id": "run-players",
            "source_file_sha256": "a" * 64,
        }
    }

    class FakeProcess:
        def __init__(self, line, exit_code):
            self.stdout = [line]
            self._exit_code = exit_code

        def wait(self):
            return self._exit_code

    responses = iter(
        [
            FakeProcess("HTTP 503 service unavailable\n", 1),
            FakeProcess("connection reset\n", 1),
            FakeProcess("Done\n", 0),
        ]
    )
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *_args, **_kwargs: next(responses))
    sleeps = []
    transient = runner.run_build(
        project_dir=project,
        artifacts_dir=tmp_path / "transient",
        target="test",
        manifest=manifest,
        sleeper=sleeps.append,
    )
    assert transient["status"] == "SUCCESS"
    assert transient["attempt_count"] == 3
    assert sleeps == [10, 20]

    monkeypatch.setattr(
        runner.subprocess,
        "Popen",
        lambda *_args, **_kwargs: FakeProcess("Failure in test critical_grain\n", 1),
    )
    quality = runner.run_build(
        project_dir=project,
        artifacts_dir=tmp_path / "quality",
        target="test",
        manifest=manifest,
        sleeper=lambda _seconds: pytest.fail("quality failure must not retry"),
    )
    assert quality["status"] == "FAILED"
    assert quality["attempt_count"] == 1
    assert quality["failure_classification"] == "permanent"


def test_silver_flow_uses_one_bronze_chain_and_no_new_schedule():
    silver = yaml.safe_load(
        (ROOT / "kestra/flows/transfermarkt_silver.yml").read_text(encoding="utf-8")
    )
    bronze = yaml.safe_load(
        (ROOT / "kestra/flows/transfermarkt_ingest_bronze.yml").read_text(encoding="utf-8")
    )

    assert silver["concurrency"] == {"limit": 1, "behavior": "QUEUE"}
    assert "triggers" not in silver
    docker_task = next(task for task in silver["tasks"] if task["id"] == "run_dbt")
    assert docker_task["type"] == "io.kestra.plugin.docker.cli.Run"
    assert docker_task["containerImage"] == "{{ envs.dbt_image }}"
    assert docker_task["runIf"] == "{{ outputs.plan.vars.should_run == true }}"
    assert "retry" not in docker_task

    silver_links = [
        task
        for task in bronze["tasks"]
        if task.get("type") == "io.kestra.plugin.core.flow.Subflow"
        and task.get("flowId") == "transfermarkt_silver"
    ]
    assert len(silver_links) == 1
    assert silver_links[0]["wait"] is True
    assert silver_links[0]["transmitFailed"] is True
    trigger = bronze["triggers"][0]
    assert trigger["cron"] == "0 6 1 * *"
    assert trigger["timezone"] == "America/Guayaquil"
    assert trigger["disabled"] is True


def test_compose_mounts_exact_project_and_docker_socket_without_key_files():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    kestra = compose["services"]["kestra"]
    volumes = kestra["volumes"]
    environment = kestra["environment"]

    assert "./dbt:/opt/transfermarkt/dbt:ro" in volumes
    assert "/var/run/docker.sock:/var/run/docker.sock" in volumes
    assert environment["DBT_IMAGE"].endswith("1.12.5-snowflake-1.12.1}")
    assert "SNOWFLAKE_PRIVATE_KEY_FILE" not in environment


def test_unified_tag_and_manual_registration_contract():
    project = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text(encoding="utf-8"))
    assert "transfermarkt_silver" in project["models"]["elt_soccer_value"]["+tags"]
    assert "transfermarkt_silver" in project["seeds"]["elt_soccer_value"]["+tags"]
    assert "transfermarkt_silver" in project["data_tests"]["elt_soccer_value"]["+tags"]

    register = (ROOT / "scripts/register-flows.ps1").read_text(encoding="utf-8")
    manual = (ROOT / "scripts/run-silver-flow.ps1").read_text(encoding="utf-8")
    assert "transfermarkt_silver.yml" in register
    assert "source_manifest" in manual
    assert "Silver backfill requires" in manual
