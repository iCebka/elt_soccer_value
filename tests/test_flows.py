from pathlib import Path

import yaml

from ingestion.cli import parser


ROOT = Path(__file__).parents[1]


def test_monthly_schedule_is_safe_and_initially_disabled():
    flow = yaml.safe_load(
        (ROOT / "kestra/flows/transfermarkt_ingest_bronze.yml").read_text(
            encoding="utf-8"
        )
    )
    trigger = flow["triggers"][0]

    assert trigger["id"] == "monthly_first_day"
    assert trigger["cron"] == "0 6 1 * *"
    assert trigger["timezone"] == "America/Guayaquil"
    assert trigger["recoverMissedSchedules"] == "NONE"
    assert trigger["disabled"] is True
    assert flow["concurrency"] == {"limit": 1, "behavior": "QUEUE"}


def test_manual_schedule_and_backfill_share_logical_date_path():
    main = (ROOT / "kestra/flows/transfermarkt_ingest_bronze.yml").read_text(
        encoding="utf-8"
    )
    child = (ROOT / "kestra/flows/transfermarkt_ingest_asset.yml").read_text(
        encoding="utf-8"
    )

    assert "trigger.date ?? execution.startDate" in main
    assert "logical_date:" in main
    assert '--logical-date "$TM_LOGICAL_DATE"' in child
    assert '--trigger-source "$TM_TRIGGER_SOURCE"' in child
    # Retries stay in Python where transient HTTP/connector errors are classified.
    assert "retry:" not in main
    assert "retry:" not in child


def test_docker_restart_does_not_replay_unbounded_history():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    embedded = compose["services"]["kestra"]["environment"]["KESTRA_CONFIGURATION"]
    configuration = yaml.safe_load(embedded)
    schedule = configuration["kestra"]["plugins"]["configurations"][0]

    assert schedule["type"] == "io.kestra.plugin.core.trigger.Schedule"
    assert schedule["values"]["recoverMissedSchedules"] == "NONE"


def test_backfill_script_has_preview_and_finite_api_contract():
    script = (ROOT / "scripts/manage-schedule.ps1").read_text(encoding="utf-8")

    assert "explicit finite RFC3339" in script
    assert "effective_query=current snapshot" in script
    assert "'/api/v1/main/triggers'" in script
    assert "'/api/v1/main/triggers/backfill/pause'" in script
    assert "'/api/v1/main/triggers/backfill/unpause'" in script
    assert "'/api/v1/main/triggers/backfill/delete'" in script
    assert "'/api/v1/main/triggers/set-disabled/by-triggers'" in script
    assert "Invoke-KestraJson 'PUT' '/api/v1/main/triggers/backfill/pause'" in script
    assert "Invoke-KestraJson 'PUT' '/api/v1/main/triggers/backfill/unpause'" in script
    assert "Invoke-KestraJson 'POST' '/api/v1/main/triggers/backfill/delete'" in script


def test_audit_command_accepts_an_explicit_limit():
    args = parser().parse_args(["audit", "--limit", "25"])

    assert args.command == "audit"
    assert args.limit == 25
