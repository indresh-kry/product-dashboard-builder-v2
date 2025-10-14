import types

import pytest

from scripts import system_health_check


def test_check_environment_infers_project(monkeypatch):
    env = {
        "RUN_HASH": "abc123",
        "DATASET_NAME": "demo_dataset.events",
    }
    report = system_health_check.check_environment(env)
    assert report.passed is True
    assert report.errors == []
    assert any("inferred project" in warning for warning in report.warnings)


def test_check_bigquery_connection_missing_credentials():
    env = {
        "GOOGLE_APPLICATION_CREDENTIALS": "",
    }
    report = system_health_check.check_bigquery_connection(env)
    assert report.passed is True
    assert report.errors == []
    assert any("Skipping BigQuery connectivity test" in warning for warning in report.warnings)


def test_check_bigquery_connection_invalid_path(tmp_path):
    env = {
        "GOOGLE_APPLICATION_CREDENTIALS": str(tmp_path / "missing.json"),
        "GOOGLE_CLOUD_PROJECT": "test-project",
    }
    report = system_health_check.check_bigquery_connection(env)
    assert report.passed is False
    assert report.errors
    assert "not found" in report.errors[0]


def test_check_bigquery_connection_success(monkeypatch, tmp_path):
    creds_path = tmp_path / "creds.json"
    creds_path.write_text("{}")

    env = {
        "GOOGLE_APPLICATION_CREDENTIALS": str(creds_path),
        "GOOGLE_CLOUD_PROJECT": "demo-project",
    }

    fake_credentials = object()

    def fake_from_file(path):
        assert path == str(creds_path)
        return fake_credentials

    class FakeJob:
        total_bytes_processed = 0

        def result(self):
            return None

    class FakeClient:
        def __init__(self, credentials=None, project=None):
            self.credentials = credentials
            self.project = project

        def query(self, query):
            assert query == "SELECT 1"
            return FakeJob()

    monkeypatch.setattr(
        system_health_check.service_account,
        "Credentials",
        types.SimpleNamespace(from_service_account_file=fake_from_file),
    )
    monkeypatch.setattr(system_health_check, "bigquery", types.SimpleNamespace(Client=FakeClient))

    report = system_health_check.check_bigquery_connection(env)
    assert report.passed is True
    assert report.errors == []
