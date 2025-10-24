#!/usr/bin/env python3
"""
Unified Analysis Workflow Orchestrator with Agentic Framework
Version: 2.1.0
Last Updated: 2025-10-24

This script serves as the unified orchestrator for the entire analysis workflow,
combining the original phases (0-4) with the agentic framework (phases 5-6).

Phases:
- 0-4: Original orchestrator (system init, schema, aggregation, segmentation, quality)
- 5-6: Agentic framework (agentic insights generation, final reporting)

Based on: analysis-workflow.md specification
Dependencies: All phase scripts in scripts/ directory + agentic framework
"""

import os
import sys
import json
import argparse
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import secrets

# Import error logger
try:
    from scripts.error_logger import get_error_logger, log_script_error
    ERROR_LOGGER_AVAILABLE = True
except ImportError:
    ERROR_LOGGER_AVAILABLE = False
    print("⚠️ Warning: Error logger not available. Install error_logger.py", file=sys.stderr)

# Add scripts directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__)))

# Import agentic framework
from agents.agentic_coordinator import AgenticCoordinator

class UnifiedAnalysisWorkflowOrchestrator:
    """
    Unified orchestrator for the analysis workflow with agentic framework.
    
    Executes phases 0-4 using the original orchestrator, then phases 5-6 using
    the agentic framework for enhanced insights generation.
    """
    
    def __init__(self):
        self.run_hash = None
        self.start_time = None
        self.phase_results = {}
        self.config = self._load_config()
        
    def _load_config(self) -> Dict:
        """Load configuration from environment and defaults."""
        return {
            'default_iterations': int(os.environ.get('DEFAULT_ITERATIONS', '5')),
            'quick_iterations': int(os.environ.get('QUICK_ITERATIONS', '3')),
            'confidence_threshold': float(os.environ.get('CONFIDENCE_THRESHOLD', '0.85')),
            'minimum_sample_size': int(os.environ.get('MINIMUM_SAMPLE_SIZE', '30')),
            'analysis_window_days': int(os.environ.get('ANALYSIS_WINDOW_DAYS', '90')),
            'statistical_significance': float(os.environ.get('STATISTICAL_SIGNIFICANCE', '0.05')),
            'max_query_cost': float(os.environ.get('MAX_QUERY_COST', '100')),
            'parallel_queries': int(os.environ.get('PARALLEL_QUERIES', '4')),
            'retry_attempts': int(os.environ.get('RETRY_ATTEMPTS', '3')),
            'timeout_seconds': int(os.environ.get('TIMEOUT_SECONDS', '3600'))
        }
    
    def _generate_run_hash(self) -> str:
        """Generate 6-character hash for run identification."""
        return secrets.token_hex(3)
    
    def _create_run_structure(self, run_hash: str) -> Dict[str, Path]:
        """Create run directory structure and return paths."""
        base_dir = Path(f"run_logs/{run_hash}")
        
        directories = {
            'base': base_dir,
            'outputs': base_dir / "outputs",
            'schema': base_dir / "outputs" / "schema",
            'segments': base_dir / "outputs" / "segments", 
            'aggregations': base_dir / "outputs" / "aggregations",
            'validation': base_dir / "outputs" / "validation",
            'reports': base_dir / "outputs" / "reports",
            'insights': base_dir / "outputs" / "insights",  # Added for agentic framework
            'raw_data': base_dir / "outputs" / "raw_data",
            'working': base_dir / "working",
            'logs': base_dir / "logs"
        }
        
        # Create all directories
        for dir_path in directories.values():
            dir_path.mkdir(parents=True, exist_ok=True)
            
        return directories
    
    def _create_run_env_file(self, run_hash: str, args: argparse.Namespace) -> Path:
        """Create run-specific environment file."""
        env_file = Path(f"run_logs/{run_hash}/.env")
        
        env_content = f"""export RUN_HASH={run_hash}
export DATASET_NAME='{args.dataset_name}'
export EVENT_NAME_COLUMN='{args.event_name_column}'
export USER_ID_COLUMN='{args.user_id_column}'
export DEVICE_ID_COLUMN='{args.device_id_column}'
export TIMESTAMP_COLUMN='{args.timestamp_column}'
export REVENUE_COLUMN='{args.revenue_column}'
export REVENUE_VALIDATION_COLUMN='{args.revenue_validation_column}'
export INSTALL_EVENTS='{args.install_events}'
export ANALYSIS_WINDOW_DAYS='{args.analysis_window_days}'
export MODE='{args.mode}'
export APP_FILTER='{args.app_filter}'
export DATE_START='{args.date_start}'
export DATE_END='{args.date_end}'
export RAW_DATA_LIMIT='{args.raw_data_limit}'
export AGGREGATION_LIMIT='{args.aggregation_limit}'
"""
        
        # Add other environment variables
        for key in ['GOOGLE_APPLICATION_CREDENTIALS', 'GOOGLE_CLOUD_PROJECT', 'BIGQUERY_READ_ONLY_MODE']:
            if key in os.environ:
                env_content += f"export {key}='{os.environ[key]}'\n"
        
        with open(env_file, 'w') as f:
            f.write(env_content)
            
        return env_file
    
    def _execute_script(self, script_name: str, run_hash: str, phase_name: str, 
                       args: List[str] = None, timeout: int = 3600) -> Tuple[bool, str]:
        """Execute a script with proper error handling and logging."""
        try:
            # Set environment variables
            env = os.environ.copy()
            env['RUN_HASH'] = run_hash
            
            # Load environment from .env file if it exists
            env_file = Path(f"run_logs/{run_hash}/.env")
            if env_file.exists():
                with open(env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            # Remove 'export ' prefix if present
                            if line.startswith('export '):
                                line = line[7:]
                            key, value = line.split('=', 1)
                            # Remove quotes if present
                            value = value.strip().strip('"').strip("'")
                            env[key] = value
            
            # Build command
            cmd = ['python3', script_name]
            if args:
                cmd.extend(args)
            
            print(f"📄 Script: {script_name}")
            if args:
                print(f"⚙️  Arguments: {' '.join(args)}")
            
            # Execute script
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
                cwd=os.getcwd()
            )
            
            if result.returncode == 0:
                print(f"✅ {phase_name} completed successfully")
                return True, result.stdout
            else:
                print(f"❌ {phase_name} failed with return code {result.returncode}")
                if result.stderr:
                    print(f"📄 Error output: {result.stderr}")
                return False, result.stderr or result.stdout
                
        except subprocess.TimeoutExpired:
            print(f"⏰ {phase_name} timed out after {timeout} seconds")
            return False, f"Script timed out after {timeout} seconds"
        except Exception as e:
            print(f"❌ {phase_name} failed with exception: {str(e)}")
            return False, str(e)
    
    def run_phase_0_system_init(self, run_hash: str, args: argparse.Namespace) -> bool:
        """Phase 0: System Initialization and Environment Setup."""
        print(f"\n🚀 Starting Phase 0: System Initialization...")
        
        # Create run structure
        directories = self._create_run_structure(run_hash)
        
        # Create environment file
        env_file = self._create_run_env_file(run_hash, args)
        
        # Log run information
        log_file = directories['logs'] / "run.log"
        with open(log_file, 'w') as f:
            f.write(f"Run {run_hash} started at {datetime.now()}\n")
            f.write(f"Dataset: {args.dataset_name}\n")
            f.write(f"Mode: {args.mode}\n")
        
        print(f"✅ Run {run_hash} initialized")
        print(f"📁 Run directory: {directories['base']}")
        print(f"📄 Environment file: {env_file}")
        print(f"✅ Phase 0: System Initialization completed successfully")
        
        return True
    
    def run_phase_1_schema_discovery(self, run_hash: str, args: argparse.Namespace) -> bool:
        """Phase 1: Schema Discovery and Field Mapping."""
        print(f"\n🚀 Starting Phase 1: Schema Discovery...")
        
        success, output = self._execute_script(
            "schema_discovery_v3.py",
            run_hash,
            "Schema Discovery",
            [
                "--app-filter", args.app_filter,
                "--date-start", args.date_start,
                "--date-end", args.date_end
            ]
        )
        
        if success:
            print(f"✅ Phase 1: Schema Discovery completed successfully")
        else:
            print(f"❌ Phase 1: Schema Discovery failed: {output}")
        
        return success
    
    def run_phase_2_data_aggregation(self, run_hash: str, args: argparse.Namespace) -> bool:
        """Phase 2: Data Aggregation and Core Metric Generation."""
        print(f"\n🚀 Starting Phase 2: Data Aggregation...")
        
        success, output = self._execute_script(
            "data_aggregation_v3.py",
            run_hash,
            "Data Aggregation",
            [
                "--app-filter", args.app_filter,
                "--date-start", args.date_start,
                "--date-end", args.date_end,
                "--raw-data-limit", str(args.raw_data_limit),
                "--aggregation-limit", str(args.aggregation_limit)
            ]
        )
        
        if success:
            print(f"✅ Phase 2: Data Aggregation completed successfully")
        else:
            print(f"❌ Phase 2: Data Aggregation failed: {output}")
        
        return success
    
    def run_phase_3_user_segmentation(self, run_hash: str, args: argparse.Namespace) -> bool:
        """Phase 3: User Segmentation with Statistical Framework."""
        print(f"\n🚀 Starting Phase 3: User Segmentation...")
        
        success, output = self._execute_script(
            "user_segmentation_v1.py",
            run_hash,
            "User Segmentation",
            [
                "--app-filter", args.app_filter,
                "--date-start", args.date_start,
                "--date-end", args.date_end
            ]
        )
        
        if success:
            print(f"✅ Phase 3: User Segmentation completed successfully")
        else:
            print(f"❌ Phase 3: User Segmentation failed: {output}")
        
        return success
    
    def run_phase_4_quality_assurance(self, run_hash: str, args: argparse.Namespace) -> bool:
        """Phase 4: Quality Assurance and Validation."""
        print(f"\n🚀 Starting Phase 4: Quality Assurance...")
        
        success, output = self._execute_script(
            "quality_validation_v1.py",
            run_hash,
            "Quality Assurance",
            [
                "--app-filter", args.app_filter,
                "--date-start", args.date_start,
                "--date-end", args.date_end
            ]
        )
        
        if success:
            print(f"✅ Phase 4: Quality Assurance completed successfully")
        else:
            print(f"❌ Phase 4: Quality Assurance failed: {output}")
        
        return success
    
    def run_phase_5_agentic_insights(self, run_hash: str, args: argparse.Namespace) -> bool:
        """Phase 5: Agentic Insights Generation."""
        print(f"\n🤖 Starting Phase 5: Agentic Insights Generation...")
        
        try:
            # Initialize agentic coordinator
            coordinator = AgenticCoordinator()
            
            # Prepare run metadata
            run_metadata = {
                'app_filter': args.app_filter,
                'start_date': args.date_start,
                'end_date': args.date_end,
                'raw_data_limit': args.raw_data_limit,
                'aggregation_limit': args.aggregation_limit
            }
            
            print(f"🤖 Running Agentic Insights Generation")
            print(f"📱 App Filter: {args.app_filter}")
            print(f"📅 Date Range: {args.date_start} to {args.date_end}")
            
            # Run agentic analysis
            results = coordinator.run_analysis(run_hash, run_metadata)
            
            print(f"✅ Agentic insights generation completed successfully")
            print(f"📊 Processed {len(results['agents_processed'])} agents")
            print(f"✅ Successful: {results['summary']['successful_agents']}")
            print(f"❌ Failed: {results['summary']['failed_agents']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Phase 5: Agentic Insights Generation failed: {str(e)}")
            return False
    
    def run_phase_6_final_reporting(self, run_hash: str, args: argparse.Namespace) -> bool:
        """Phase 6: Final Report Generation."""
        print(f"\n📊 Starting Phase 6: Final Report Generation...")
        
        try:
            # Create final summary
            summary = {
                "run_hash": run_hash,
                "start_time": self.start_time.isoformat(),
                "end_time": datetime.now().isoformat(),
                "duration_seconds": (datetime.now() - self.start_time).total_seconds(),
                "app_filter": args.app_filter,
                "date_range": f"{args.date_start} to {args.date_end}",
                "raw_data_limit": args.raw_data_limit,
                "aggregation_limit": args.aggregation_limit,
                "phases_completed": len([p for p in self.phase_results.values() if p]),
                "total_phases": len(self.phase_results),
                "success": all(self.phase_results.values()),
                "phase_results": self.phase_results
            }
            
            # Save summary
            summary_file = Path(f"run_logs/{run_hash}/outputs/reports/unified_workflow_summary.json")
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"✅ Phase 6: Final Report Generation completed successfully")
            print(f"📄 Summary saved to: {summary_file}")
            
            return True
            
        except Exception as e:
            print(f"❌ Phase 6: Final Report Generation failed: {str(e)}")
            return False
    
    def run_complete_workflow(self, args: argparse.Namespace) -> bool:
        """Run the complete unified workflow."""
        # Generate run hash and start time
        self.run_hash = self._generate_run_hash()
        self.start_time = datetime.now()
        
        print(f"🚀 Starting Unified Analysis Workflow Orchestrator")
        print(f"==========================================")
        print(f"Run Hash: {self.run_hash}")
        print(f"Start Time: {self.start_time}")
        print(f"Mode: {args.mode}")
        print(f"Dataset: {args.dataset_name}")
        print(f"App Filter: {args.app_filter}")
        print(f"Date Range: {args.date_start} to {args.date_end}")
        print(f"Raw Data Limit: {args.raw_data_limit}")
        print(f"Aggregation Limit: {args.aggregation_limit}")
        
        # Run all phases
        phases = [
            ("Phase 0: System Initialization", self.run_phase_0_system_init),
            ("Phase 1: Schema Discovery", self.run_phase_1_schema_discovery),
            ("Phase 2: Data Aggregation", self.run_phase_2_data_aggregation),
            ("Phase 3: User Segmentation", self.run_phase_3_user_segmentation),
            ("Phase 4: Quality Assurance", self.run_phase_4_quality_assurance),
            ("Phase 5: Agentic Insights Generation", self.run_phase_5_agentic_insights),
            ("Phase 6: Final Report Generation", self.run_phase_6_final_reporting)
        ]
        
        for phase_name, phase_func in phases:
            try:
                success = phase_func(self.run_hash, args)
                self.phase_results[phase_name] = success
                
                if not success and phase_name in ["Phase 0: System Initialization", "Phase 1: Schema Discovery", "Phase 2: Data Aggregation"]:
                    print(f"\n❌ Workflow stopped due to failure in {phase_name}")
                    break
                    
            except Exception as e:
                print(f"\n❌ {phase_name} failed with exception: {str(e)}")
                self.phase_results[phase_name] = False
                break
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - self.start_time
        successful_phases = sum(self.phase_results.values())
        total_phases = len(self.phase_results)
        
        print(f"\n🎉 Unified Analysis Workflow Orchestrator Complete")
        print(f"==========================================")
        print(f"Run Hash: {self.run_hash}")
        print(f"Duration: {duration}")
        print(f"Status: {'✅ SUCCESS' if all(self.phase_results.values()) else '❌ FAILED'}")
        print(f"Results: {successful_phases}/{total_phases} phases successful")
        
        # Show phase results
        for phase_name, success in self.phase_results.items():
            status = "✅" if success else "❌"
            print(f"  {status} {phase_name}")
        
        return all(self.phase_results.values())

def main():
    """Main entry point for the unified orchestrator."""
    parser = argparse.ArgumentParser(
        description="Unified Analysis Workflow Orchestrator with Agentic Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analysis_workflow_orchestrator_unified.py --app-filter com.example.app --date-start 2025-01-01 --date-end 2025-01-31
  python analysis_workflow_orchestrator_unified.py --app-filter com.example.app --date-start 2025-01-01 --date-end 2025-01-31 --raw-data-limit 1000 --aggregation-limit 100000
        """
    )
    
    # Required arguments
    parser.add_argument('--app-filter', required=True, help='App filter for analysis')
    parser.add_argument('--date-start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date-end', required=True, help='End date (YYYY-MM-DD)')
    
    # Optional arguments
    parser.add_argument('--raw-data-limit', type=int, default=100, help='Raw data limit (default: 100)')
    parser.add_argument('--aggregation-limit', type=int, default=100000, help='Aggregation limit (default: 100000)')
    parser.add_argument('--mode', default='full', choices=['full', 'quick', 'schema-only', 'aggregation-only'], help='Analysis mode (default: full)')
    
    # Dataset configuration
    parser.add_argument('--dataset-name', default='gc-prod-459709.nbs_dataset.singular_user_level_event_data', help='BigQuery dataset name')
    parser.add_argument('--event-name-column', default='name', help='Event name column')
    parser.add_argument('--user-id-column', default='custom_user_id', help='User ID column')
    parser.add_argument('--device-id-column', default='device_id', help='Device ID column')
    parser.add_argument('--timestamp-column', default='adjusted_timestamp', help='Timestamp column')
    parser.add_argument('--revenue-column', default='converted_revenue', help='Revenue column')
    parser.add_argument('--revenue-validation-column', default='is_revenue_event', help='Revenue validation column')
    parser.add_argument('--install-events', default='install,first_launch,af_first_launch', help='Install events')
    parser.add_argument('--analysis-window-days', type=int, default=90, help='Analysis window in days')
    
    args = parser.parse_args()
    
    # Create and run orchestrator
    orchestrator = UnifiedAnalysisWorkflowOrchestrator()
    success = orchestrator.run_complete_workflow(args)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
