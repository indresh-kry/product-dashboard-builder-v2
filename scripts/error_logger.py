#!/usr/bin/env python3
"""
Error Logger for Product Dashboard Builder v2
Version: 1.0.0
Last Updated: 2025-10-23

This module provides centralized error logging for all scripts in the analysis workflow.
It creates dedicated error log files in run folders and provides structured error reporting.

Dependencies:
- json: JSON serialization
- os: Environment variable access
- datetime: Timestamp generation
- traceback: Stack trace capture
"""

import os
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import traceback

class ErrorLogger:
    """Centralized error logging for analysis workflow."""
    
    def __init__(self, run_hash: str):
        self.run_hash = run_hash
        self.error_log_path = Path(f"run_logs/{run_hash}/logs/errors.log")
        self.bug_log_path = Path(f"run_logs/{run_hash}/logs/bug_report.json")
        self._ensure_log_directory()
    
    def _ensure_log_directory(self):
        """Ensure the logs directory exists."""
        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def log_error(self, 
                  script_name: str, 
                  error_type: str, 
                  error_message: str, 
                  error_details: Optional[Dict] = None,
                  stack_trace: Optional[str] = None) -> None:
        """
        Log an error to the error log file.
        
        Args:
            script_name: Name of the script that failed
            error_type: Type of error (e.g., 'ValidationError', 'APIError', 'DataError')
            error_message: Human-readable error message
            error_details: Additional error context
            stack_trace: Full stack trace if available
        """
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "run_hash": self.run_hash,
            "script_name": script_name,
            "error_type": error_type,
            "error_message": error_message,
            "error_details": error_details or {},
            "stack_trace": stack_trace,
            "environment": {
                "python_version": sys.version,
                "working_directory": os.getcwd(),
                "environment_variables": {
                    "RUN_HASH": os.environ.get('RUN_HASH'),
                    "DATASET_NAME": os.environ.get('DATASET_NAME'),
                    "USER_ID_COLUMN": os.environ.get('USER_ID_COLUMN'),
                    "DEVICE_ID_COLUMN": os.environ.get('DEVICE_ID_COLUMN')
                }
            }
        }
        
        # Append to error log file
        with open(self.error_log_path, 'a') as f:
            f.write(f"{json.dumps(error_entry, indent=2)}\n\n")
        
        # Also print to stderr for immediate visibility
        print(f"❌ ERROR [{script_name}]: {error_message}", file=sys.stderr)
        if error_details:
            print(f"🔍 Details: {json.dumps(error_details, indent=2)}", file=sys.stderr)
    
    def log_script_failure(self, 
                           script_name: str, 
                           return_code: int, 
                           stderr_output: str, 
                           stdout_output: str = "") -> None:
        """Log a script execution failure."""
        self.log_error(
            script_name=script_name,
            error_type="ScriptExecutionError",
            error_message=f"Script failed with return code {return_code}",
            error_details={
                "return_code": return_code,
                "stderr": stderr_output,
                "stdout": stdout_output
            }
        )
    
    def log_validation_error(self, 
                             script_name: str, 
                             validation_type: str, 
                             validation_error: str) -> None:
        """Log a validation error."""
        self.log_error(
            script_name=script_name,
            error_type="ValidationError",
            error_message=f"{validation_type} validation failed",
            error_details={
                "validation_type": validation_type,
                "validation_error": validation_error
            }
        )
    
    def log_api_error(self, 
                      script_name: str, 
                      api_name: str, 
                      api_error: str) -> None:
        """Log an API error."""
        self.log_error(
            script_name=script_name,
            error_type="APIError",
            error_message=f"{api_name} API call failed",
            error_details={
                "api_name": api_name,
                "api_error": api_error
            }
        )
    
    def log_data_error(self, 
                       script_name: str, 
                       data_source: str, 
                       data_error: str) -> None:
        """Log a data-related error."""
        self.log_error(
            script_name=script_name,
            error_type="DataError",
            error_message=f"Data processing failed for {data_source}",
            error_details={
                "data_source": data_source,
                "data_error": data_error
            }
        )
    
    def create_bug_report(self) -> Dict[str, Any]:
        """Create a comprehensive bug report from all errors."""
        if not self.error_log_path.exists():
            return {"errors": [], "summary": "No errors found"}
        
        errors = []
        with open(self.error_log_path, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        error = json.loads(line)
                        errors.append(error)
                    except json.JSONDecodeError:
                        continue
        
        # Generate summary
        error_types = {}
        script_failures = {}
        
        for error in errors:
            error_type = error.get('error_type', 'Unknown')
            script_name = error.get('script_name', 'Unknown')
            
            error_types[error_type] = error_types.get(error_type, 0) + 1
            script_failures[script_name] = script_failures.get(script_name, 0) + 1
        
        bug_report = {
            "generated_at": datetime.now().isoformat(),
            "run_hash": self.run_hash,
            "total_errors": len(errors),
            "error_summary": {
                "by_type": error_types,
                "by_script": script_failures
            },
            "errors": errors,
            "recommendations": self._generate_recommendations(errors)
        }
        
        # Save bug report
        with open(self.bug_log_path, 'w') as f:
            json.dump(bug_report, f, indent=2)
        
        return bug_report
    
    def _generate_recommendations(self, errors: list) -> list:
        """Generate recommendations based on error patterns."""
        recommendations = []
        
        # Check for common error patterns
        api_errors = [e for e in errors if e.get('error_type') == 'APIError']
        validation_errors = [e for e in errors if e.get('error_type') == 'ValidationError']
        data_errors = [e for e in errors if e.get('error_type') == 'DataError']
        
        if api_errors:
            recommendations.append({
                "category": "API Issues",
                "recommendation": "Check API credentials and rate limits",
                "priority": "High",
                "affected_scripts": list(set(e.get('script_name') for e in api_errors))
            })
        
        if validation_errors:
            recommendations.append({
                "category": "Data Validation",
                "recommendation": "Review data quality and schema mappings",
                "priority": "Medium",
                "affected_scripts": list(set(e.get('script_name') for e in validation_errors))
            })
        
        if data_errors:
            recommendations.append({
                "category": "Data Processing",
                "recommendation": "Verify data sources and query parameters",
                "priority": "High",
                "affected_scripts": list(set(e.get('script_name') for e in data_errors))
            })
        
        return recommendations

def get_error_logger(run_hash: str) -> ErrorLogger:
    """Get an error logger instance for the current run."""
    return ErrorLogger(run_hash)

# Convenience functions for common error logging
def log_script_error(script_name: str, error: Exception, run_hash: str = None):
    """Log a script error with automatic stack trace capture."""
    if run_hash is None:
        run_hash = os.environ.get('RUN_HASH', 'unknown')
    
    logger = get_error_logger(run_hash)
    logger.log_error(
        script_name=script_name,
        error_type=type(error).__name__,
        error_message=str(error),
        stack_trace=traceback.format_exc()
    )

def log_validation_failure(script_name: str, validation_type: str, error: str, run_hash: str = None):
    """Log a validation failure."""
    if run_hash is None:
        run_hash = os.environ.get('RUN_HASH', 'unknown')
    
    logger = get_error_logger(run_hash)
    logger.log_validation_error(script_name, validation_type, error)

if __name__ == "__main__":
    # Test the error logger
    test_run_hash = "test_error_logging"
    logger = get_error_logger(test_run_hash)
    
    # Test different error types
    logger.log_error("test_script.py", "TestError", "This is a test error")
    logger.log_script_failure("test_script.py", 1, "Test stderr output")
    logger.log_validation_error("test_script.py", "SchemaValidation", "Schema validation failed")
    
    # Create bug report
    bug_report = logger.create_bug_report()
    print(f"Bug report created with {bug_report['total_errors']} errors")
