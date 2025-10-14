#!/usr/bin/env python3
"""System Health Check Script for Product Dashboard Builder v2.
Version: 2.1.0
Last Updated: 2025-10-14

Changelog:
- v2.1.0 (2025-10-14): Enhanced with merged Phase 1 improvements
- v2.0.0 (2025-10-14): Added dataclass structure and improved validation
- v1.0.0 (2025-10-14): Initial version

Environment Variables:
- RUN_HASH: Unique identifier for the current run
- DATASET_NAME: BigQuery dataset name
- GOOGLE_CLOUD_PROJECT: Google Cloud project ID
- GOOGLE_APPLICATION_CREDENTIALS: Path to service account credentials

Dependencies:
- google-cloud-bigquery: BigQuery client library
- google-oauth2: OAuth2 authentication
"""

#!/usr/bin/env python3
"""System Health Check Script for Product Dashboard Builder v2."""
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.oauth2 import service_account


@dataclass
class CheckReport:
    """Structured result for a health check step."""

    passed: bool
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def _infer_project_id(dataset_name: Optional[str]) -> Optional[str]:
    """Infer project id from a dataset name of the form `project.dataset`."""
    if not dataset_name or "." not in dataset_name:
        return None
    return dataset_name.split(".", 1)[0]


def check_environment(env: Optional[Dict[str, str]] = None) -> CheckReport:
    """Check if the required environment variables are set and actionable."""
    env = env or os.environ
    warnings: List[str] = []
    errors: List[str] = []

    required_vars = ["RUN_HASH", "DATASET_NAME"]
    missing_required = [var for var in required_vars if not env.get(var)]
    if missing_required:
        errors.append(
            "Missing critical environment variables: "
            + ", ".join(missing_required)
        )

    project_id = env.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        inferred = _infer_project_id(env.get("DATASET_NAME"))
        if inferred:
            warnings.append(
                "GOOGLE_CLOUD_PROJECT not set – inferred project "
                f"'{inferred}' from DATASET_NAME."
            )
            if env is os.environ:
                os.environ.setdefault("GOOGLE_CLOUD_PROJECT", inferred)
        else:
            warnings.append(
                "GOOGLE_CLOUD_PROJECT not set and could not be inferred. "
                "BigQuery checks will continue but may fail."
            )

    credentials_path = env.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        warnings.append(
            "GOOGLE_APPLICATION_CREDENTIALS not set – BigQuery connectivity "
            "check will be skipped."
        )

    return CheckReport(passed=not errors, warnings=warnings, errors=errors)


def check_bigquery_connection(env: Optional[Dict[str, str]] = None) -> CheckReport:
    """Check BigQuery connectivity with actionable diagnostics."""
    env = env or os.environ
    warnings: List[str] = []
    errors: List[str] = []

    credentials_path = env.get("GOOGLE_APPLICATION_CREDENTIALS")
    project_id = env.get("GOOGLE_CLOUD_PROJECT")

    if not credentials_path:
        warnings.append(
            "Skipping BigQuery connectivity test – GOOGLE_APPLICATION_CREDENTIALS "
            "is not configured."
        )
        return CheckReport(passed=True, warnings=warnings, errors=errors)

    if not os.path.exists(credentials_path):
        errors.append(
            f"Credentials file '{credentials_path}' not found. "
            "Please verify the path or rerun Phase 0 setup."
        )
        return CheckReport(passed=False, warnings=warnings, errors=errors)

    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        client = bigquery.Client(credentials=credentials, project=project_id)
        client.query("SELECT 1").result()
        return CheckReport(passed=True, warnings=warnings, errors=errors)
    except Exception as exc:  # pragma: no cover - pass details through
        errors.append(f"BigQuery connection failed: {exc}")
        return CheckReport(passed=False, warnings=warnings, errors=errors)


def _emit_report(prefix: str, messages: List[str]) -> None:
    for message in messages:
        print(f"{prefix} {message}")


def main() -> int:
    """Main health check entrypoint."""
    print("🔍 Running system health checks...")

    env_report = check_environment()
    _emit_report("⚠️", env_report.warnings)
    _emit_report("❌", env_report.errors)

    bq_report = check_bigquery_connection()
    _emit_report("⚠️", bq_report.warnings)
    _emit_report("❌", bq_report.errors)

    if env_report.passed and bq_report.passed:
        print("✅ All system health checks passed")
        return 0

    print("❌ Some system health checks failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
