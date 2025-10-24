#!/usr/bin/env python3
"""Rules engine integration for Phase 1 schema discovery outputs."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Dict, List, Optional


STATUS_PRECEDENCE = {"pass": 0, "warn": 1, "fail": 2}


def _determine_status(checks: List[Dict[str, object]]) -> str:
    """Aggregate overall status from individual checks."""
    if not checks:
        return "warn"
    return max(checks, key=lambda item: STATUS_PRECEDENCE[item["status"]])["status"]


def _confidence(values: List[float]) -> float:
    """Return bounded average confidence."""
    if not values:
        return 0.0
    return round(max(0.0, min(1.0, mean(values))), 2)


def _check_primary_identifier(
    schema_mapping: Dict[str, object],
    user_rules: Dict[str, object],
    schema_columns: Dict[str, Dict[str, object]],
) -> Dict[str, object]:
    primary_field = user_rules.get("hierarchy", [{}])[0].get("field")
    confidence = user_rules.get("hierarchy", [{}])[0].get("confidence", 0.5)
    if not primary_field:
        return {
            "id": "user-primary-field",
            "status": "fail",
            "confidence": confidence,
            "details": "User identification hierarchy is empty.",
            "recommendation": "Ensure schema discovery populates hierarchy with at least one field.",
        }

    if primary_field in schema_columns:
        status = "pass"
        details = f"Primary identifier '{primary_field}' present in schema."
        recommendation = None
    else:
        status = "fail"
        details = (
            f"Primary identifier '{primary_field}' missing from schema_info listing."
        )
        recommendation = (
            "Verify schema discovery captured the column and review dataset permissions."
        )

    return {
        "id": "user-primary-field",
        "status": status,
        "confidence": confidence,
        "details": details,
        "recommendation": recommendation,
    }


def _check_revenue_fields(
    revenue_schemas: Dict[str, Dict[str, object]],
    schema_columns: Dict[str, Dict[str, object]],
) -> Dict[str, object]:
    missing = []
    analysed_fields = []
    for key in ("iap", "admon"):
        schema = revenue_schemas.get(key) or {}
        field_name = schema.get("revenue_field")
        if field_name:
            analysed_fields.append(field_name)
            if field_name not in schema_columns:
                missing.append((key, field_name))

    if missing:
        return {
            "id": "revenue-field-coverage",
            "status": "fail",
            "confidence": 0.4,
            "details": "Revenue fields missing from schema info: "
            + ", ".join(f"{source}:{field}" for source, field in missing),
            "recommendation": "Confirm revenue columns exist in the dataset and rerun discovery.",
        }

    if not analysed_fields:
        return {
            "id": "revenue-field-coverage",
            "status": "warn",
            "confidence": 0.3,
            "details": "No revenue fields detected for IAP/AdMon. Downstream aggregation may fail.",
            "recommendation": "Review naming conventions or extend heuristics in schema discovery.",
        }

    return {
        "id": "revenue-field-coverage",
        "status": "pass",
        "confidence": 0.6,
        "details": f"Revenue fields analysed: {', '.join(sorted(set(analysed_fields)))}.",
        "recommendation": None,
    }


def _check_data_quality_score(schema_mapping: Dict[str, object]) -> Dict[str, object]:
    score = float(schema_mapping.get("data_quality_score") or 0)
    if score >= 80:
        status = "pass"
        recommendation = None
    elif score >= 60:
        status = "warn"
        recommendation = (
            "Investigate columns with high null rates before using aggregations in production."
        )
    else:
        status = "fail"
        recommendation = (
            "Data quality score below 60%. Address missing data and rerun discovery."
        )

    return {
        "id": "data-quality-score",
        "status": status,
        "confidence": round(score / 100, 2),
        "details": f"Overall data quality score: {score}%.",
        "recommendation": recommendation,
    }


def _check_event_taxonomy(event_taxonomy: List[Dict[str, object]]) -> Dict[str, object]:
    categories = {record.get("category", "uncategorized") for record in event_taxonomy}
    if not event_taxonomy:
        return {
            "id": "event-taxonomy-coverage",
            "status": "warn",
            "confidence": 0.2,
            "details": "No events found in taxonomy output with current filters.",
            "recommendation": "Relax filters or confirm dataset activity within the analysis window.",
        }

    required_categories = {"monetization", "progression", "engagement"}
    missing_categories = required_categories.difference(categories)
    if missing_categories:
        return {
            "id": "event-taxonomy-coverage",
            "status": "warn",
            "confidence": 0.4,
            "details": (
                "Missing event categories: " + ", ".join(sorted(missing_categories))
            ),
            "recommendation": (
                "Confirm events are classified correctly or extend keyword mappings."
            ),
        }

    return {
        "id": "event-taxonomy-coverage",
        "status": "pass",
        "confidence": 0.7,
        "details": (
            f"Event categories covered: {', '.join(sorted(categories))}."
        ),
        "recommendation": None,
    }


def validate_schema_mapping(
    schema_mapping: Dict[str, object],
    output_dir: str,
    *,
    user_rules: Optional[Dict[str, object]] = None,
    event_taxonomy: Optional[List[Dict[str, object]]] = None,
    revenue_schemas: Optional[Dict[str, Dict[str, object]]] = None,
) -> Dict[str, object]:
    """Validate schema mapping and produce structured rules-engine output."""
    user_rules = user_rules or schema_mapping.get("user_identification", {}).get("rules", {})
    event_taxonomy = event_taxonomy or []
    revenue_schemas = revenue_schemas or schema_mapping.get("revenue_calculation", {})

    schema_columns = {
        column.get("name"): column for column in schema_mapping.get("schema_info", [])
    }

    checks = [
        _check_primary_identifier(schema_mapping, user_rules, schema_columns),
        _check_revenue_fields(revenue_schemas, schema_columns),
        _check_data_quality_score(schema_mapping),
        _check_event_taxonomy(event_taxonomy),
    ]

    overall_status = _determine_status(checks)
    confidence_score = _confidence([check["confidence"] for check in checks])
    recommendations = [
        check["recommendation"] for check in checks if check.get("recommendation")
    ]

    payload = {
        "generated_at": datetime.now().isoformat(),
        "output_dir": output_dir,
        "status": overall_status,
        "confidence": confidence_score,
        "checks": checks,
        "recommendations": recommendations,
    }
    return payload


def _load_json(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text())


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate schema mapping outputs.")
    parser.add_argument(
        "--schema-mapping",
        required=True,
        type=Path,
        help="Path to schema_mapping.json",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Directory where validation results will be stored",
    )
    parser.add_argument(
        "--user-rules",
        type=Path,
        help="Optional path to user_identification_rules.json",
    )
    parser.add_argument(
        "--event-taxonomy",
        type=Path,
        help="Optional path to event_taxonomy.json",
    )
    parser.add_argument(
        "--revenue-iap",
        type=Path,
        help="Optional path to iap_revenue_schema.json",
    )
    parser.add_argument(
        "--revenue-admon",
        type=Path,
        help="Optional path to admon_revenue_schema.json",
    )

    args = parser.parse_args()

    schema_mapping = _load_json(args.schema_mapping)
    user_rules = _load_json(args.user_rules) if args.user_rules else None

    event_taxonomy = []
    if args.event_taxonomy and args.event_taxonomy.exists():
        event_taxonomy = json.loads(args.event_taxonomy.read_text())

    revenue_schemas = {}
    if args.revenue_iap and args.revenue_iap.exists():
        revenue_schemas["iap"] = json.loads(args.revenue_iap.read_text())
    if args.revenue_admon and args.revenue_admon.exists():
        revenue_schemas["admon"] = json.loads(args.revenue_admon.read_text())

    payload = validate_schema_mapping(
        schema_mapping,
        output_dir=str(args.output_dir),
        user_rules=user_rules,
        event_taxonomy=event_taxonomy,
        revenue_schemas=revenue_schemas or None,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "rules_validation.json").write_text(json.dumps(payload, indent=2))
    print(f"Validation written to {args.output_dir / 'rules_validation.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
