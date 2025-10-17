#!/usr/bin/env python3
"""
Multi-LLM Insights Generation Script
Version: 1.0.0
Last Updated: 2025-10-16

Description:
Main script that orchestrates the multi-LLM architecture for generating comprehensive insights.
Coordinates 6 specialized child LLMs and 1 parent coordinator LLM.

Dependencies:
- All child LLM scripts
- llm_coordinator_v1.py
- json: JSON serialization
- os: Environment variable access
- datetime: Timestamp generation
"""
import os
import json
import subprocess
import sys
from datetime import datetime
from typing import Dict, List
from pathlib import Path

def run_child_llm(script_name: str, run_hash: str, run_metadata: Dict) -> Dict:
    """Run a child LLM script and return its output."""
    print(f"🚀 Running {script_name}...")
    
    try:
        # Set environment variables for the child script
        env = os.environ.copy()
        env['RUN_HASH'] = run_hash
        
        # Run the child script
        result = subprocess.run(
            [sys.executable, f'scripts/{script_name}'],
            capture_output=True,
            text=True,
            env=env,
            cwd=os.getcwd()
        )
        
        if result.returncode == 0:
            print(f"✅ {script_name} completed successfully")
            # Try to parse JSON output from stdout
            try:
                # Extract JSON from the output (look for the insights section)
                output_lines = result.stdout.split('\n')
                json_start = -1
                for i, line in enumerate(output_lines):
                    if '📊 Insights:' in line or 'Insights:' in line:
                        json_start = i + 1
                        break
                
                if json_start >= 0 and json_start < len(output_lines):
                    json_str = '\n'.join(output_lines[json_start:])
                    return json.loads(json_str)
                else:
                    # Fallback: try to parse the entire stdout as JSON
                    return json.loads(result.stdout)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code blocks
                import re
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', result.stdout, re.DOTALL)
                if json_match:
                    try:
                        return json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass
                
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', result.stdout, re.DOTALL)
                if json_match:
                    try:
                        return json.loads(json_match.group(0))
                    except json.JSONDecodeError:
                        pass
                
                # If all parsing fails, return a basic structure
                return {
                    "raw_output": result.stdout,
                    "parsing_error": "Could not parse JSON output",
                    "script": script_name
                }
        else:
            print(f"❌ {script_name} failed with return code {result.returncode}")
            print(f"Error: {result.stderr}")
            return {
                "error": f"Script failed with return code {result.returncode}",
                "stderr": result.stderr,
                "script": script_name
            }
            
    except Exception as e:
        print(f"❌ Error running {script_name}: {str(e)}")
        return {
            "error": str(e),
            "script": script_name
        }

def run_parent_coordinator(child_outputs: Dict, run_metadata: Dict) -> Dict:
    """Run the parent coordinator LLM."""
    print("🎯 Running parent coordinator...")
    
    try:
        # Import and run the coordinator
        sys.path.append('scripts')
        from llm_coordinator_v1 import coordinate_insights
        
        coordinated_insights = coordinate_insights(child_outputs, run_metadata)
        print("✅ Parent coordinator completed successfully")
        return coordinated_insights
        
    except Exception as e:
        print(f"❌ Error running parent coordinator: {str(e)}")
        return {
            "error": str(e),
            "coordinator": "parent_llm"
        }

def save_insights_outputs(coordinated_insights: Dict, child_outputs: Dict, run_hash: str):
    """Save all insights outputs to the run directory."""
    print("💾 Saving insights outputs...")
    
    outputs_dir = f'run_logs/{run_hash}/outputs/insights'
    os.makedirs(outputs_dir, exist_ok=True)
    
    # Save coordinated insights
    with open(f'{outputs_dir}/coordinated_insights.json', 'w') as f:
        json.dump(coordinated_insights, f, indent=2, default=str)
    
    # Save individual child outputs
    with open(f'{outputs_dir}/child_insights.json', 'w') as f:
        json.dump(child_outputs, f, indent=2, default=str)
    
    # Save execution summary
    execution_summary = {
        "run_hash": run_hash,
        "execution_timestamp": datetime.now().isoformat(),
        "architecture": "multi_llm",
        "child_analysts": list(child_outputs.keys()),
        "coordinator_status": "completed" if "error" not in coordinated_insights else "failed",
        "total_analysts": len(child_outputs),
        "successful_analysts": len([k for k, v in child_outputs.items() if "error" not in v])
    }
    
    with open(f'{outputs_dir}/execution_summary.json', 'w') as f:
        json.dump(execution_summary, f, indent=2, default=str)
    
    print(f"✅ Insights saved to: {outputs_dir}")

def main():
    """Main multi-LLM insights generation function."""
    print("🚀 Starting Multi-LLM Insights Generation v1.0.0")
    print("=" * 80)
    
    # Get run hash
    run_hash = os.environ.get('RUN_HASH')
    if not run_hash:
        print("❌ RUN_HASH environment variable not set")
        return 1
    
    # Prepare run metadata
    run_metadata = {
        'run_hash': run_hash,
        'date_range': f"{os.environ.get('DATE_START', 'unknown')} to {os.environ.get('DATE_END', 'unknown')}",
        'data_source': 'phase_3_fallback',
        'total_users': os.environ.get('AGGREGATION_LIMIT', 'unknown'),
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"Run Hash: {run_hash}")
    print(f"Date Range: {run_metadata['date_range']}")
    print(f"Data Source: {run_metadata['data_source']}")
    print()
    
    # Define child LLM scripts
    child_scripts = {
        'daily_metrics': 'llm_child_daily_metrics_v1.py',
        'user_segmentation': 'llm_child_user_segmentation_v1.py',
        'geographic': 'llm_child_geographic_v1.py',
        'cohort_retention': 'llm_child_cohort_retention_v1.py',
        'revenue_optimization': 'llm_child_revenue_optimization_v1.py',
        'data_quality': 'llm_child_data_quality_v1.py'
    }
    
    # Run all child LLMs in parallel (simulated with sequential for now)
    print("🔄 Running child LLMs...")
    child_outputs = {}
    
    for analyst_name, script_name in child_scripts.items():
        child_outputs[analyst_name] = run_child_llm(script_name, run_hash, run_metadata)
        print()
    
    # Run parent coordinator
    print("🎯 Running parent coordinator...")
    coordinated_insights = run_parent_coordinator(child_outputs, run_metadata)
    print()
    
    # Save outputs
    save_insights_outputs(coordinated_insights, child_outputs, run_hash)
    
    # Print summary
    print("\n🎉 Multi-LLM Insights Generation completed!")
    print(f"📊 Coordinated insights generated: {'Yes' if 'error' not in coordinated_insights else 'No'}")
    print(f"👥 Child analysts completed: {len([k for k, v in child_outputs.items() if 'error' not in v])}/{len(child_outputs)}")
    print(f"📁 Outputs saved to: run_logs/{run_hash}/outputs/insights/")
    
    return 0

if __name__ == "__main__":
    exit(main())
