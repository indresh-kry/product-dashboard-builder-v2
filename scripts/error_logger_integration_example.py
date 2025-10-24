#!/usr/bin/env python3
"""
Example of how to integrate error logging into existing scripts.
This shows how to modify the revenue optimization script to use centralized error logging.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict

# Import the error logger
try:
    from scripts.error_logger import get_error_logger, log_script_error, log_validation_failure
    ERROR_LOGGER_AVAILABLE = True
except ImportError:
    ERROR_LOGGER_AVAILABLE = False
    print("⚠️ Warning: Error logger not available. Install error_logger.py", file=sys.stderr)

def example_revenue_optimization_with_error_logging():
    """Example of how to integrate error logging into revenue optimization script."""
    
    run_hash = os.environ.get('RUN_HASH', 'unknown')
    
    # Initialize error logger
    if ERROR_LOGGER_AVAILABLE:
        error_logger = get_error_logger(run_hash)
    
    try:
        # Your existing script logic here
        print("💰 Analyzing revenue optimization...", file=sys.stderr)
        
        # Simulate some work that might fail
        if not os.path.exists(f'run_logs/{run_hash}/outputs/segments'):
            raise FileNotFoundError("Revenue segments data not found")
        
        # Simulate API call that might fail
        try:
            # Your API call logic here
            pass
        except Exception as api_error:
            if ERROR_LOGGER_AVAILABLE:
                error_logger.log_api_error(
                    script_name="llm_child_revenue_optimization_v1.py",
                    api_name="OpenAI",
                    api_error=str(api_error)
                )
            raise
        
        # Simulate validation that might fail
        try:
            # Your validation logic here
            pass
        except Exception as validation_error:
            if ERROR_LOGGER_AVAILABLE:
                error_logger.log_validation_error(
                    script_name="llm_child_revenue_optimization_v1.py",
                    validation_type="SchemaValidation",
                    error=str(validation_error)
                )
            raise
        
        print("✅ Revenue optimization analysis completed!", file=sys.stderr)
        return 0
        
    except Exception as e:
        # Log the error using the centralized logger
        if ERROR_LOGGER_AVAILABLE:
            log_script_error("llm_child_revenue_optimization_v1.py", e, run_hash)
        
        print(f"❌ Error during analysis: {str(e)}", file=sys.stderr)
        return 1

def example_orchestrator_integration():
    """Example of how to integrate error logging into the orchestrator."""
    
    def _execute_script_with_error_logging(self, script_path: str, run_hash: str, phase_name: str):
        """Enhanced script execution with error logging."""
        try:
            # Your existing script execution logic here
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                return True, result.stdout
            else:
                # Log script failure
                if ERROR_LOGGER_AVAILABLE:
                    error_logger = get_error_logger(run_hash)
                    error_logger.log_script_failure(
                        script_name=os.path.basename(script_path),
                        return_code=result.returncode,
                        stderr_output=result.stderr,
                        stdout_output=result.stdout
                    )
                
                return False, f"Script failed with return code {result.returncode}: {result.stderr}"
                
        except subprocess.TimeoutExpired:
            if ERROR_LOGGER_AVAILABLE:
                error_logger = get_error_logger(run_hash)
                error_logger.log_error(
                    script_name=os.path.basename(script_path),
                    error_type="TimeoutError",
                    error_message=f"Script timed out after 300 seconds"
                )
            return False, "Script timed out after 300 seconds"
        except Exception as e:
            if ERROR_LOGGER_AVAILABLE:
                log_script_error(os.path.basename(script_path), e, run_hash)
            return False, f"Error executing script: {str(e)}"

if __name__ == "__main__":
    # Test the error logging integration
    os.environ['RUN_HASH'] = 'test_error_integration'
    example_revenue_optimization_with_error_logging()
