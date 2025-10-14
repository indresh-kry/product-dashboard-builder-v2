from types import SimpleNamespace
from pathlib import Path

import pandas as pd

from scripts import schema_discovery_enhanced


class FakeField(SimpleNamespace):
    pass


class FakeQueryResult:
    def __init__(self, dataframe: pd.DataFrame):
        self._df = dataframe

    def to_dataframe(self):
        return self._df


class FakeClient:
    def __init__(self):
        self.apps_df = pd.DataFrame(
            [
                {
                    "app_longname": "Demo App",
                    "event_count": 100,
                    "earliest_date": "2024-01-01",
                    "latest_date": "2024-01-07",
                }
            ]
        )
        self.date_range_df = pd.DataFrame(
            [
                {
                    "earliest_date": "2024-01-01",
                    "latest_date": "2024-01-07",
                    "total_events": 100,
                }
            ]
        )

    def get_table(self, dataset_name):
        return SimpleNamespace(
            schema=[
                FakeField(name="custom_user_id", field_type="STRING", mode="NULLABLE", description=""),
                FakeField(name="app_longname", field_type="STRING", mode="NULLABLE", description=""),
                FakeField(name="adjusted_timestamp", field_type="TIMESTAMP", mode="NULLABLE", description=""),
            ]
        )

    def query(self, query):
        if "GROUP BY app_longname" in query:
            return FakeQueryResult(self.apps_df.copy())
        if "COUNT(*) as total_events" in query:
            return FakeQueryResult(self.date_range_df.copy())
        # Force empty dataframes for sampling and taxonomy to trigger edge path
        return FakeQueryResult(pd.DataFrame())


def test_no_data_flow_creates_required_outputs(monkeypatch, tmp_path):
    run_hash = "testrun"
    dataset_name = "demo.project"

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RUN_HASH", run_hash)
    monkeypatch.setenv("DATASET_NAME", dataset_name)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "creds.json"))
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "demo-project")

    (tmp_path / "creds.json").write_text("{}")

    monkeypatch.setattr(schema_discovery_enhanced, "get_bigquery_client", lambda: FakeClient())

    exit_code = schema_discovery_enhanced.main()
    assert exit_code == 1  # no data warning path

    outputs_dir = Path(f"run_logs/{run_hash}/outputs/schema")
    assert (outputs_dir / "schema_mapping.json").exists()
    assert (outputs_dir / "event_taxonomy.json").exists()
    assert (outputs_dir / "iap_revenue_schema.json").exists()
    assert (outputs_dir / "admon_revenue_schema.json").exists()
    assert (outputs_dir / "user_identification_rules.json").exists()
    assert (outputs_dir / "rules_validation.json").exists()


def test_build_revenue_schemas_detects_event_categories():
    taxonomy_records = [
        {"event_name": "iap_purchase_complete", "category": "monetization"},
        {"event_name": "ad_impression_shown", "category": "monetization"},
        {"event_name": "level_completed", "category": "progression"},
    ]
    revenue_columns = [
        {"column_name": "converted_revenue", "null_percentage": 0},
        {"column_name": "ad_revenue", "null_percentage": 0},
        {"column_name": "is_revenue_event", "null_percentage": 0},
    ]
    quality_metrics = {
        "converted_revenue": {"null_percentage": 5, "unique_values": 10},
        "ad_revenue": {"null_percentage": 2, "unique_values": 8},
    }

    iap_schema, admon_schema = schema_discovery_enhanced.build_revenue_schemas(
        taxonomy_records, revenue_columns, quality_metrics
    )

    assert "iap_purchase_complete" in iap_schema["events"]
    assert iap_schema["revenue_field"] == "converted_revenue"
    assert iap_schema["data_quality_notes"] is not None

    assert "ad_impression_shown" in admon_schema["events"]
    assert admon_schema["revenue_field"] == "ad_revenue"
