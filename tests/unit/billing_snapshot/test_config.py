"""Tests for billing_snapshot.config env-var reads."""

import importlib


def test_defaults(monkeypatch):
    for var in (
        "ATHENA_WORKGROUP",
        "MAPPINGS_BUCKET",
        "MAPPINGS_KEY",
        "OUTPUT_BUCKET",
        "OUTPUT_KEY",
        "ATHENA_DATABASE",
        "ATHENA_TABLE",
        "HISTORY_START_DATE",
        "CHUNK_COUNT",
        "MAX_WORKERS",
        "POLL_INTERVAL_SECONDS",
        "POLL_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(var, raising=False)

    import config as cfg

    importlib.reload(cfg)
    assert cfg.ATHENA_WORKGROUP == "sbm-billing-snapshot"
    assert cfg.MAPPINGS_BUCKET == "sbm-file-ingester"
    assert cfg.MAPPINGS_KEY == "nem12_mappings.json"
    assert cfg.OUTPUT_BUCKET == "gegoptimareports"
    assert cfg.OUTPUT_KEY == "bunnings-billing/billing-latest.csv"
    assert cfg.ATHENA_DATABASE == "default"
    assert cfg.ATHENA_TABLE == "sensordata_default"
    assert cfg.HISTORY_START_DATE == "2025-01-01"
    assert cfg.CHUNK_COUNT == 8
    assert cfg.MAX_WORKERS == 3
    assert cfg.POLL_INTERVAL_SECONDS == 2
    assert cfg.POLL_TIMEOUT_SECONDS == 240


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("ATHENA_WORKGROUP", "custom-wg")
    monkeypatch.setenv("CHUNK_COUNT", "16")
    monkeypatch.setenv("MAX_WORKERS", "5")

    import config as cfg

    importlib.reload(cfg)
    assert cfg.ATHENA_WORKGROUP == "custom-wg"
    assert cfg.CHUNK_COUNT == 16
    assert cfg.MAX_WORKERS == 5
