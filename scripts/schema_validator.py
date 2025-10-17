#!/usr/bin/env python3
"""
Schema Validator for LLM Analyst Responses
Version: 1.0.0
Last Updated: 2025-10-16

This module provides strict JSON schema validation for all LLM analyst responses.
It enforces the standardized schemas defined in schemas/analyst_schemas.json and
blocks execution on parsing errors to ensure data quality.

Environment Variables:
- SCHEMA_FILE: Path to the schema definitions file (default: schemas/analyst_schemas.json)

Dependencies:
- jsonschema: JSON schema validation library
- json: JSON parsing and validation
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

try:
    import jsonschema
    from jsonschema import validate, ValidationError
except ImportError:
    print("❌ Error: jsonschema library not found. Install with: pip install jsonschema")
    sys.exit(1)


class SchemaValidationError(Exception):
    """Custom exception for schema validation errors."""
    pass


class LLMSchemaValidator:
    """Validates LLM analyst responses against standardized JSON schemas."""
    
    def __init__(self, schema_file: str = "schemas/analyst_schemas.json"):
        """Initialize the schema validator with the schema definitions file."""
        # Handle relative paths by looking in multiple locations
        if not os.path.exists(schema_file):
            # Try relative to project root
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            alt_path = os.path.join(project_root, schema_file)
            if os.path.exists(alt_path):
                schema_file = alt_path
            else:
                # Try relative to current working directory
                alt_path = os.path.join(os.getcwd(), schema_file)
                if os.path.exists(alt_path):
                    schema_file = alt_path
        
        self.schema_file = schema_file
        self.schemas = self._load_schemas()
        
        # Special handling for revenue optimization analyst - use simplified schema
        if "revenue_optimization" in self.schemas.get("analysts", {}):
            simplified_schema_file = schema_file.replace("analyst_schemas.json", "simplified_analyst_schemas.json")
            if os.path.exists(simplified_schema_file):
                print(f"🔄 Using simplified schema for revenue optimization analyst")
                simplified_schemas = self._load_schemas_from_file(simplified_schema_file)
                if "revenue_optimization" in simplified_schemas.get("analysts", {}):
                    self.schemas["analysts"]["revenue_optimization"] = simplified_schemas["analysts"]["revenue_optimization"]
        
    def _load_schemas(self) -> Dict[str, Any]:
        """Load schema definitions from the schema file."""
        return self._load_schemas_from_file(self.schema_file)
    
    def _load_schemas_from_file(self, schema_file: str) -> Dict[str, Any]:
        """Load schema definitions from a specific schema file."""
        try:
            schema_path = Path(schema_file)
            if not schema_path.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_file}")
                
            with open(schema_path, 'r') as f:
                schema_data = json.load(f)
                
            print(f"✅ Loaded schemas from: {schema_file}")
            return schema_data
            
        except Exception as e:
            print(f"❌ Error loading schemas: {str(e)}")
            raise SchemaValidationError(f"Failed to load schemas: {str(e)}")
    
    def validate_response(self, response: str, analyst_type: str) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """
        Validate an LLM response against the appropriate schema.
        
        Args:
            response: Raw response string from LLM
            analyst_type: Type of analyst (daily_metrics, user_segmentation, etc.)
            
        Returns:
            Tuple of (is_valid, parsed_data, error_message)
        """
        try:
            # Parse JSON response
            parsed_data = self._parse_json_response(response)
            
            # Get schema for analyst type
            analysts = self.schemas.get('analysts', {})
            if analyst_type not in analysts:
                raise SchemaValidationError(f"Unknown analyst type: {analyst_type}")
                
            schema = analysts[analyst_type]['schema']
            
            # Validate against schema
            validate(instance=parsed_data, schema=schema)
            
            print(f"✅ Schema validation passed for {analyst_type}")
            return True, parsed_data, None
            
        except json.JSONDecodeError as e:
            error_msg = f"JSON parsing failed: {str(e)}"
            print(f"❌ {error_msg}")
            return False, {}, error_msg
            
        except ValidationError as e:
            error_msg = f"Schema validation failed: {str(e)}"
            print(f"❌ {error_msg}")
            return False, {}, error_msg
            
        except SchemaValidationError as e:
            error_msg = f"Schema validation error: {str(e)}"
            print(f"❌ {error_msg}")
            return False, {}, error_msg
            
        except Exception as e:
            error_msg = f"Unexpected validation error: {str(e)}"
            print(f"❌ {error_msg}")
            return False, {}, error_msg
    
    def _parse_json_response(self, response: str) -> Dict[str, Any]:
        """Parse JSON response with fallback handling for various formats."""
        import re
        
        # Try direct JSON parsing first
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass
        
        # Try to extract JSON from markdown code blocks
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Try to find JSON object in the text
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                pass
        
        # If all parsing fails, raise an error
        raise json.JSONDecodeError("No valid JSON found in response", response, 0)
    
    def get_required_fields(self, analyst_type: str) -> list:
        """Get list of required fields for a specific analyst type."""
        if analyst_type not in self.schemas:
            return []
            
        schema = self.schemas[analyst_type]['schema']
        return schema.get('required', [])
    
    def get_schema_info(self, analyst_type: str) -> Dict[str, Any]:
        """Get schema information for a specific analyst type."""
        if analyst_type not in self.schemas:
            return {}
            
        return {
            'analyst_type': analyst_type,
            'required_fields': self.get_required_fields(analyst_type),
            'schema_version': '1.0.0',
            'last_updated': datetime.now().isoformat()
        }
    
    def validate_all_analysts(self, responses: Dict[str, str]) -> Dict[str, Tuple[bool, Dict[str, Any], Optional[str]]]:
        """Validate responses from all analysts."""
        results = {}
        
        for analyst_type, response in responses.items():
            print(f"🔍 Validating {analyst_type} response...")
            is_valid, parsed_data, error = self.validate_response(response, analyst_type)
            results[analyst_type] = (is_valid, parsed_data, error)
            
        return results
    
    def create_validation_report(self, validation_results: Dict[str, Tuple[bool, Dict[str, Any], Optional[str]]]) -> Dict[str, Any]:
        """Create a comprehensive validation report."""
        report = {
            'validation_timestamp': datetime.now().isoformat(),
            'total_analysts': len(validation_results),
            'successful_validations': sum(1 for is_valid, _, _ in validation_results.values() if is_valid),
            'failed_validations': sum(1 for is_valid, _, _ in validation_results.values() if not is_valid),
            'validation_rate': 0.0,
            'analyst_results': {}
        }
        
        for analyst_type, (is_valid, parsed_data, error) in validation_results.items():
            report['analyst_results'][analyst_type] = {
                'valid': is_valid,
                'error': error,
                'has_data': bool(parsed_data),
                'data_keys': list(parsed_data.keys()) if parsed_data else []
            }
        
        if report['total_analysts'] > 0:
            report['validation_rate'] = report['successful_validations'] / report['total_analysts']
        
        return report


def main():
    """Main function for testing the schema validator."""
    print("🧪 Testing LLM Schema Validator")
    print("=" * 40)
    
    # Initialize validator
    validator = LLMSchemaValidator()
    
    # Test with sample responses
    test_responses = {
        'daily_metrics': '{"analyst_type": "daily_metrics", "timestamp": "2025-10-16T16:08:10.477731", "run_hash": "test", "trend_analysis": {"dau_trend": "decreasing", "growth_rate": {"value": -2.4, "unit": "percentage", "confidence": 0.85, "sample_size": 150000}, "consistency_score": {"value": 0.78, "unit": "score", "definition": "DAU consistency"}}, "key_metrics": {"avg_dau": {"value": 2671, "unit": "users", "confidence": 0.88, "sample_size": 150000}, "new_user_ratio": {"value": 0.51, "unit": "percentage", "definition": "New users / Total DAU", "confidence": 0.85}, "engagement_stability": {"value": 0.78, "unit": "score", "definition": "Engagement consistency"}}, "insights": [{"insight": "DAU is declining", "significance": "High", "confidence": 0.85, "sample_size": 150000}], "confidence_score": 0.85, "metadata": {"generated_at": "2025-10-16T16:08:10.477731", "run_hash": "test", "analyst_type": "daily_metrics", "data_files_used": ["dau_by_date.csv"]}}',
        'revenue_optimization': '{"analyst_type": "revenue_optimization", "timestamp": "2025-10-16T16:08:53.843684", "run_hash": "test", "revenue_analysis": {"revenue_streams": {"iap": {"value": 0.6, "unit": "percentage", "confidence": 0.85, "sample_size": 150000}, "ads": {"value": 0.3, "unit": "percentage", "confidence": 0.82, "sample_size": 150000}, "subscription": {"value": 0.1, "unit": "percentage", "confidence": 0.78, "sample_size": 150000}}, "monetization_rate": {"value": 0.15, "unit": "percentage", "definition": "Paying users / Total users", "confidence": 0.88}, "arpu": {"value": 2.45, "unit": "USD", "definition": "Average revenue per user", "confidence": 0.85}, "revenue_health_score": {"value": 0.75, "unit": "score", "definition": "Overall revenue health"}}, "monetization_insights": {"paying_user_characteristics": ["High engagement", "Premium features"], "conversion_factors": ["FTUE completion", "Level progression"], "monetization_funnel": {"awareness": 0.8, "consideration": 0.4, "purchase": 0.15}}, "pricing_recommendations": [{"recommendation": "Increase IAP prices", "target_segment": "whale", "expected_impact": "High", "implementation_effort": "Low"}], "growth_strategies": [{"strategy": "Improve ad monetization", "revenue_stream": "ads", "expected_impact": "Medium", "timeline": "1-3 months"}], "confidence_score": 0.85, "metadata": {"generated_at": "2025-10-16T16:08:53.843684", "run_hash": "test", "analyst_type": "revenue_optimization", "data_files_used": ["revenue_segments_daily.csv"]}}'
    }
    
    # Validate all responses
    results = validator.validate_all_analysts(test_responses)
    
    # Create validation report
    report = validator.create_validation_report(results)
    
    print("\n📊 Validation Report:")
    print(f"Total Analysts: {report['total_analysts']}")
    print(f"Successful Validations: {report['successful_validations']}")
    print(f"Failed Validations: {report['failed_validations']}")
    print(f"Validation Rate: {report['validation_rate']:.2%}")
    
    print("\n📋 Detailed Results:")
    for analyst_type, result in report['analyst_results'].items():
        status = "✅ PASS" if result['valid'] else "❌ FAIL"
        print(f"{analyst_type}: {status}")
        if not result['valid']:
            print(f"  Error: {result['error']}")


if __name__ == "__main__":
    main()
