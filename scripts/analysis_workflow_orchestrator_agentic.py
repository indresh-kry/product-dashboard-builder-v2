#!/usr/bin/env python3
"""
Analysis Workflow Orchestrator with Agentic Framework
Version: 2.0.0
Last Updated: 2025-10-23

Modified orchestrator that uses the agentic framework for insights generation
instead of individual child LLM scripts.
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import hashlib

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from agents.agentic_coordinator import AgenticCoordinator

class AgenticWorkflowOrchestrator:
    """Orchestrator that uses agentic framework for insights generation."""
    
    def __init__(self):
        self.run_hash = self._generate_run_hash()
        self.start_time = datetime.now()
        
        # Set environment variables
        os.environ['RUN_HASH'] = self.run_hash
        
        # Create run directory
        self.run_dir = Path(f"run_logs/{self.run_hash}")
        self.run_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"🚀 Starting Agentic Workflow Orchestrator")
        print(f"📁 Run Hash: {self.run_hash}")
        print(f"📁 Run Directory: {self.run_dir}")
        
    def _generate_run_hash(self) -> str:
        """Generate a unique run hash."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return hashlib.md5(timestamp.encode()).hexdigest()[:8]
    
    def run_phase(self, phase_name: str, script_path: str, args: list) -> bool:
        """Run a single phase of the workflow."""
        print(f"\n🔄 Running Phase: {phase_name}")
        print(f"📄 Script: {script_path}")
        print(f"⚙️  Arguments: {' '.join(args)}")
        
        try:
            # Set environment variables for the script
            env = os.environ.copy()
            env['RUN_HASH'] = self.run_hash
            
            # Run the script
            result = subprocess.run(
                [sys.executable, script_path] + args,
                capture_output=True,
                text=True,
                env=env,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                print(f"✅ Phase {phase_name} completed successfully")
                return True
            else:
                print(f"❌ Phase {phase_name} failed with return code {result.returncode}")
                print(f"📄 Error output: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Phase {phase_name} timed out after 5 minutes")
            return False
        except Exception as e:
            print(f"❌ Error running phase {phase_name}: {str(e)}")
            return False
    
    def run_agentic_insights(self, app_filter: str, date_start: str, date_end: str) -> bool:
        """Run insights generation using the agentic framework."""
        print(f"\n🤖 Running Agentic Insights Generation")
        print(f"📱 App Filter: {app_filter}")
        print(f"📅 Date Range: {date_start} to {date_end}")
        
        try:
            # Create agentic coordinator
            coordinator = AgenticCoordinator()
            
            # Prepare metadata
            run_metadata = {
                'app_filter': app_filter,
                'date_start': date_start,
                'date_end': date_end,
                'timestamp': datetime.now().isoformat(),
                'orchestrator': 'agentic_workflow'
            }
            
            # Run agentic analysis
            results = coordinator.run_analysis(self.run_hash, run_metadata)
            
            # Check if analysis was successful
            if results['summary']['successful_agents'] > 0:
                print(f"✅ Agentic insights generation completed successfully")
                print(f"📊 Processed {results['summary']['total_agents']} agents")
                print(f"✅ Successful: {results['summary']['successful_agents']}")
                print(f"❌ Failed: {results['summary']['failed_agents']}")
                return True
            else:
                print(f"❌ Agentic insights generation failed")
                print(f"🚨 Errors: {results['summary']['errors']}")
                return False
                
        except Exception as e:
            print(f"❌ Error in agentic insights generation: {str(e)}")
            return False
    
    def run_workflow(self, app_filter: str, date_start: str, date_end: str, 
                    raw_data_limit: int, aggregation_limit: int) -> bool:
        """Run the complete workflow with agentic insights."""
        print(f"\n🎯 Starting Complete Workflow with Agentic Framework")
        print(f"📱 App Filter: {app_filter}")
        print(f"📅 Date Range: {date_start} to {date_end}")
        print(f"📊 Raw Data Limit: {raw_data_limit}")
        print(f"📊 Aggregation Limit: {aggregation_limit}")
        
        all_success = True
        
        # Phase 1: Data Aggregation
        print(f"\n📊 Phase 1: Data Aggregation")
        phase1_success = self.run_phase(
            "Data Aggregation",
            "scripts/data_aggregation_v3.py",
            [
                "--app-filter", app_filter,
                "--date-start", date_start,
                "--date-end", date_end,
                "--raw-data-limit", str(raw_data_limit),
                "--aggregation-limit", str(aggregation_limit)
            ]
        )
        
        if not phase1_success:
            print("❌ Data aggregation failed")
            all_success = False
        else:
            print("✅ Data aggregation completed")
        
        # Phase 2: Data Segmentation
        print(f"\n📊 Phase 2: Data Segmentation")
        phase2_success = self.run_phase(
            "Data Segmentation",
            "scripts/user_segmentation_v1.py",
            [
                "--app-filter", app_filter,
                "--date-start", date_start,
                "--date-end", date_end
            ]
        )
        
        if not phase2_success:
            print("❌ Data segmentation failed")
            all_success = False
        else:
            print("✅ Data segmentation completed")
        
        # Phase 3: Agentic Insights Generation
        print(f"\n🤖 Phase 3: Agentic Insights Generation")
        phase3_success = self.run_agentic_insights(app_filter, date_start, date_end)
        
        if not phase3_success:
            print("❌ Agentic insights generation failed")
            all_success = False
        else:
            print("✅ Agentic insights generation completed")
        
        # Generate final summary
        self._generate_final_summary(all_success)
        
        return all_success
    
    def _generate_final_summary(self, success: bool):
        """Generate final summary of the workflow."""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        summary = {
            'run_hash': self.run_hash,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration.total_seconds(),
            'success': success,
            'orchestrator_type': 'agentic_workflow',
            'phases': {
                'data_aggregation': 'completed',
                'data_segmentation': 'completed',
                'agentic_insights': 'completed'
            }
        }
        
        # Save summary
        summary_path = self.run_dir / "outputs" / "reports" / "agentic_workflow_summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"\n📊 Final Summary:")
        print(f"  Run Hash: {self.run_hash}")
        print(f"  Duration: {duration.total_seconds():.2f} seconds")
        print(f"  Success: {'✅' if success else '❌'}")
        print(f"  Summary saved to: {summary_path}")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Agentic Workflow Orchestrator')
    parser.add_argument('--app-filter', required=True, help='App filter for analysis')
    parser.add_argument('--date-start', required=True, help='Start date for analysis')
    parser.add_argument('--date-end', required=True, help='End date for analysis')
    parser.add_argument('--raw-data-limit', type=int, default=100, help='Raw data limit')
    parser.add_argument('--aggregation-limit', type=int, default=100000, help='Aggregation limit')
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = AgenticWorkflowOrchestrator()
    
    # Run workflow
    success = orchestrator.run_workflow(
        app_filter=args.app_filter,
        date_start=args.date_start,
        date_end=args.date_end,
        raw_data_limit=args.raw_data_limit,
        aggregation_limit=args.aggregation_limit
    )
    
    if success:
        print("\n🎉 Agentic workflow completed successfully!")
        return 0
    else:
        print("\n❌ Agentic workflow failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
