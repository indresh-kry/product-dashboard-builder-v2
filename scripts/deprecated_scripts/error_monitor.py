#!/usr/bin/env python3
"""
Error Monitor for Product Dashboard Builder v2
Version: 1.0.0
Last Updated: 2025-10-23

This script provides error monitoring and analysis capabilities for the analysis workflow.
It can analyze error patterns across multiple runs and provide insights for debugging.

Usage:
    python3 scripts/error_monitor.py --run-hash <hash>          # Analyze specific run
    python3 scripts/error_monitor.py --all-runs                # Analyze all runs
    python3 scripts/error_monitor.py --pattern-analysis        # Find error patterns
"""

import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter

class ErrorMonitor:
    """Monitor and analyze errors across analysis runs."""
    
    def __init__(self):
        self.run_logs_dir = Path("run_logs")
    
    def analyze_run_errors(self, run_hash: str) -> Dict[str, Any]:
        """Analyze errors for a specific run."""
        error_log_path = self.run_logs_dir / run_hash / "logs" / "errors.log"
        bug_report_path = self.run_logs_dir / run_hash / "logs" / "bug_report.json"
        
        if not error_log_path.exists():
            return {"error": f"No error log found for run {run_hash}"}
        
        errors = []
        with open(error_log_path, 'r') as f:
            content = f.read().strip()
            if content:
                # Try to parse as single JSON object first
                try:
                    error = json.loads(content)
                    errors.append(error)
                except json.JSONDecodeError:
                    # Try parsing line by line
                    for line in content.split('\n'):
                        if line.strip():
                            try:
                                error = json.loads(line)
                                errors.append(error)
                            except json.JSONDecodeError:
                                continue
        
        # Generate analysis
        analysis = {
            "run_hash": run_hash,
            "total_errors": len(errors),
            "error_types": Counter(e.get('error_type', 'Unknown') for e in errors),
            "script_errors": Counter(e.get('script_name', 'Unknown') for e in errors),
            "error_timeline": self._create_error_timeline(errors),
            "most_common_errors": self._find_most_common_errors(errors),
            "recommendations": self._generate_recommendations(errors)
        }
        
        return analysis
    
    def analyze_all_runs(self) -> Dict[str, Any]:
        """Analyze errors across all runs."""
        all_errors = []
        run_analyses = {}
        
        # Find all run directories
        for run_dir in self.run_logs_dir.iterdir():
            if run_dir.is_dir():
                error_log_path = run_dir / "logs" / "errors.log"
                if error_log_path.exists():
                    run_hash = run_dir.name
                    run_analysis = self.analyze_run_errors(run_hash)
                    run_analyses[run_hash] = run_analysis
                    
                    # Collect all errors
                    with open(error_log_path, 'r') as f:
                        for line in f:
                            if line.strip():
                                try:
                                    error = json.loads(line)
                                    error['run_hash'] = run_hash
                                    all_errors.append(error)
                                except json.JSONDecodeError:
                                    continue
        
        # Generate cross-run analysis
        analysis = {
            "total_runs_analyzed": len(run_analyses),
            "total_errors_across_runs": len(all_errors),
            "runs_with_errors": len([r for r in run_analyses.values() if r.get('total_errors', 0) > 0]),
            "error_frequency": Counter(e.get('error_type', 'Unknown') for e in all_errors),
            "script_frequency": Counter(e.get('script_name', 'Unknown') for e in all_errors),
            "recent_errors": self._get_recent_errors(all_errors),
            "error_patterns": self._find_error_sequences(all_errors),
            "run_analyses": run_analyses
        }
        
        return analysis
    
    def find_error_patterns(self) -> Dict[str, Any]:
        """Find patterns in errors across all runs."""
        all_errors = []
        
        # Collect all errors
        for run_dir in self.run_logs_dir.iterdir():
            if run_dir.is_dir():
                error_log_path = run_dir / "logs" / "errors.log"
                if error_log_path.exists():
                    with open(error_log_path, 'r') as f:
                        for line in f:
                            if line.strip():
                                try:
                                    error = json.loads(line)
                                    error['run_hash'] = run_dir.name
                                    all_errors.append(error)
                                except json.JSONDecodeError:
                                    continue
        
        patterns = {
            "recurring_errors": self._find_recurring_errors(all_errors),
            "error_sequences": self._find_error_sequences(all_errors),
            "time_patterns": self._analyze_time_patterns(all_errors),
            "script_correlations": self._analyze_script_correlations(all_errors),
            "environment_factors": self._analyze_environment_factors(all_errors)
        }
        
        return patterns
    
    def _create_error_timeline(self, errors: List[Dict]) -> List[Dict]:
        """Create a timeline of errors."""
        timeline = []
        for error in errors:
            timeline.append({
                "timestamp": error.get('timestamp'),
                "script": error.get('script_name'),
                "error_type": error.get('error_type'),
                "message": error.get('error_message')
            })
        
        return sorted(timeline, key=lambda x: x['timestamp'])
    
    def _find_most_common_errors(self, errors: List[Dict]) -> List[Dict]:
        """Find the most common error messages."""
        error_counts = Counter(e.get('error_message', 'Unknown') for e in errors)
        return [{"error": error, "count": count} for error, count in error_counts.most_common(5)]
    
    def _generate_recommendations(self, errors: List[Dict]) -> List[Dict]:
        """Generate recommendations based on error patterns."""
        recommendations = []
        
        # Check for API errors
        api_errors = [e for e in errors if e.get('error_type') == 'APIError']
        if api_errors:
            recommendations.append({
                "category": "API Issues",
                "priority": "High",
                "recommendation": "Check API credentials and rate limits",
                "affected_scripts": list(set(e.get('script_name') for e in api_errors))
            })
        
        # Check for validation errors
        validation_errors = [e for e in errors if e.get('error_type') == 'ValidationError']
        if validation_errors:
            recommendations.append({
                "category": "Data Validation",
                "priority": "Medium",
                "recommendation": "Review data quality and schema mappings",
                "affected_scripts": list(set(e.get('script_name') for e in validation_errors))
            })
        
        # Check for timeout errors
        timeout_errors = [e for e in errors if e.get('error_type') == 'TimeoutError']
        if timeout_errors:
            recommendations.append({
                "category": "Performance",
                "priority": "Medium",
                "recommendation": "Consider increasing timeout values or optimizing queries",
                "affected_scripts": list(set(e.get('script_name') for e in timeout_errors))
            })
        
        return recommendations
    
    def _get_recent_errors(self, all_errors: List[Dict], hours: int = 24) -> List[Dict]:
        """Get errors from the last N hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_errors = []
        
        for error in all_errors:
            try:
                error_time = datetime.fromisoformat(error.get('timestamp', ''))
                if error_time > cutoff_time:
                    recent_errors.append(error)
            except (ValueError, TypeError):
                continue
        
        return recent_errors
    
    def _find_recurring_errors(self, all_errors: List[Dict]) -> List[Dict]:
        """Find errors that occur across multiple runs."""
        error_by_run = defaultdict(set)
        
        for error in all_errors:
            error_key = f"{error.get('script_name')}:{error.get('error_type')}:{error.get('error_message')}"
            error_by_run[error_key].add(error.get('run_hash'))
        
        recurring = []
        for error_key, runs in error_by_run.items():
            if len(runs) > 1:
                script, error_type, message = error_key.split(':', 2)
                recurring.append({
                    "script": script,
                    "error_type": error_type,
                    "message": message,
                    "affected_runs": list(runs),
                    "frequency": len(runs)
                })
        
        return sorted(recurring, key=lambda x: x['frequency'], reverse=True)
    
    def _find_error_sequences(self, all_errors: List[Dict]) -> List[Dict]:
        """Find sequences of errors that commonly occur together."""
        # Group errors by run and timestamp
        run_errors = defaultdict(list)
        for error in all_errors:
            run_errors[error.get('run_hash')].append(error)
        
        # Find common sequences
        sequences = defaultdict(int)
        for run_hash, errors in run_errors.items():
            # Sort by timestamp
            sorted_errors = sorted(errors, key=lambda x: x.get('timestamp', ''))
            
            # Look for 2-error sequences
            for i in range(len(sorted_errors) - 1):
                seq = f"{sorted_errors[i].get('error_type')} -> {sorted_errors[i+1].get('error_type')}"
                sequences[seq] += 1
        
        return [{"sequence": seq, "frequency": freq} for seq, freq in sequences.items() if freq > 1]
    
    def _analyze_time_patterns(self, all_errors: List[Dict]) -> Dict[str, Any]:
        """Analyze when errors occur."""
        hourly_counts = defaultdict(int)
        daily_counts = defaultdict(int)
        
        for error in all_errors:
            try:
                error_time = datetime.fromisoformat(error.get('timestamp', ''))
                hourly_counts[error_time.hour] += 1
                daily_counts[error_time.strftime('%Y-%m-%d')] += 1
            except (ValueError, TypeError):
                continue
        
        return {
            "hourly_distribution": dict(hourly_counts),
            "daily_distribution": dict(daily_counts),
            "peak_error_hour": max(hourly_counts.items(), key=lambda x: x[1])[0] if hourly_counts else None,
            "peak_error_day": max(daily_counts.items(), key=lambda x: x[1])[0] if daily_counts else None
        }
    
    def _analyze_script_correlations(self, all_errors: List[Dict]) -> Dict[str, Any]:
        """Analyze which scripts tend to fail together."""
        run_scripts = defaultdict(set)
        
        for error in all_errors:
            run_scripts[error.get('run_hash')].add(error.get('script_name'))
        
        # Find scripts that fail together
        script_pairs = defaultdict(int)
        for scripts in run_scripts.values():
            if len(scripts) > 1:
                script_list = list(scripts)
                for i in range(len(script_list)):
                    for j in range(i+1, len(script_list)):
                        pair = tuple(sorted([script_list[i], script_list[j]]))
                        script_pairs[pair] += 1
        
        return {
            "script_correlations": [{"scripts": list(pair), "frequency": freq} 
                                   for pair, freq in script_pairs.items() if freq > 1]
        }
    
    def _analyze_environment_factors(self, all_errors: List[Dict]) -> Dict[str, Any]:
        """Analyze environment factors that might contribute to errors."""
        env_factors = {
            "python_versions": Counter(),
            "working_directories": Counter(),
            "missing_env_vars": Counter()
        }
        
        for error in all_errors:
            env = error.get('environment', {})
            env_factors["python_versions"][env.get('python_version', 'Unknown')] += 1
            env_factors["working_directories"][env.get('working_directory', 'Unknown')] += 1
            
            # Check for missing environment variables
            env_vars = env.get('environment_variables', {})
            for var in ['RUN_HASH', 'DATASET_NAME', 'USER_ID_COLUMN', 'DEVICE_ID_COLUMN']:
                if not env_vars.get(var):
                    env_factors["missing_env_vars"][var] += 1
        
        return env_factors

def main():
    """Main entry point for error monitoring."""
    parser = argparse.ArgumentParser(description="Error Monitor for Product Dashboard Builder v2")
    parser.add_argument('--run-hash', type=str, help='Analyze errors for specific run')
    parser.add_argument('--all-runs', action='store_true', help='Analyze errors across all runs')
    parser.add_argument('--pattern-analysis', action='store_true', help='Find error patterns')
    parser.add_argument('--output', type=str, help='Output file for analysis results')
    
    args = parser.parse_args()
    
    monitor = ErrorMonitor()
    
    if args.run_hash:
        analysis = monitor.analyze_run_errors(args.run_hash)
        print(f"Error Analysis for Run: {args.run_hash}")
        print("=" * 50)
        print(f"Total Errors: {analysis.get('total_errors', 0)}")
        print(f"Error Types: {dict(analysis.get('error_types', {}))}")
        print(f"Script Errors: {dict(analysis.get('script_errors', {}))}")
        
    elif args.all_runs:
        analysis = monitor.analyze_all_runs()
        print("Cross-Run Error Analysis")
        print("=" * 50)
        print(f"Total Runs Analyzed: {analysis.get('total_runs_analyzed', 0)}")
        print(f"Runs with Errors: {analysis.get('runs_with_errors', 0)}")
        print(f"Total Errors: {analysis.get('total_errors_across_runs', 0)}")
        print(f"Error Frequency: {dict(analysis.get('error_frequency', {}))}")
        
    elif args.pattern_analysis:
        patterns = monitor.find_error_patterns()
        print("Error Pattern Analysis")
        print("=" * 50)
        print(f"Recurring Errors: {len(patterns.get('recurring_errors', []))}")
        print(f"Error Sequences: {len(patterns.get('error_sequences', []))}")
        
        # Show top recurring errors
        recurring = patterns.get('recurring_errors', [])[:5]
        if recurring:
            print("\nTop Recurring Errors:")
            for error in recurring:
                print(f"  {error['script']}: {error['error_type']} (affects {error['frequency']} runs)")
    
    else:
        print("Please specify --run-hash, --all-runs, or --pattern-analysis")
        return 1
    
    # Save output if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(analysis if 'analysis' in locals() else patterns, f, indent=2)
        print(f"\nAnalysis saved to: {args.output}")
    
    return 0

if __name__ == "__main__":
    exit(main())
