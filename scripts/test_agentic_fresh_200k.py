#!/usr/bin/env python3
"""
Test Agentic Framework with Fresh 200k Data
Version: 2.0.0
Last Updated: 2025-10-23

Test script to validate the agentic framework with fresh data from run 012df8 (200k aggregation limit).
"""

import os
import sys
import json
from pathlib import Path

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from agents.agentic_coordinator import AgenticCoordinator

def test_with_fresh_200k_data():
    """Test the agentic coordinator with fresh 200k data from run 012df8."""
    print("🧪 Testing Agentic Framework with Fresh 200k Data from Run 012df8...")
    
    # Use the fresh run hash with 200k aggregation
    run_hash = "012df8"
    
    # Set environment
    os.environ['RUN_HASH'] = run_hash
    
    # Create coordinator
    coordinator = AgenticCoordinator()
    
    # Prepare metadata
    run_metadata = {
        'app_filter': 'com.nukebox.mandir',
        'date_start': '2025-09-15',
        'date_end': '2025-09-30',
        'timestamp': '2025-10-23T18:40:00Z',
        'aggregation_limit': '200000'
    }
    
    print(f"  📊 Run Hash: {run_hash}")
    print(f"  📱 App Filter: {run_metadata['app_filter']}")
    print(f"  📅 Date Range: {run_metadata['date_start']} to {run_metadata['date_end']}")
    print(f"  📊 Aggregation Limit: {run_metadata['aggregation_limit']}")
    
    # Check if data exists
    data_dir = Path(f"../run_logs/{run_hash}/outputs")
    if not data_dir.exists():
        print(f"  ❌ Data directory not found: {data_dir}")
        return False
    
    print(f"  ✅ Data directory exists: {data_dir}")
    
    # Check what data files are available
    csv_files = list(data_dir.rglob("*.csv"))
    json_files = list(data_dir.rglob("*.json"))
    print(f"  📊 Found {len(csv_files)} CSV files and {len(json_files)} JSON files")
    
    # Run analysis
    try:
        results = coordinator.run_analysis(run_hash, run_metadata)
        
        print(f"  ✅ Analysis completed successfully")
        print(f"  📈 Agents processed: {len(results['agents_processed'])}")
        print(f"  ✅ Successful agents: {results['summary']['successful_agents']}")
        print(f"  ❌ Failed agents: {results['summary']['failed_agents']}")
        print(f"  🚨 Errors: {results['summary']['errors']}")
        
        # Show agent results
        for agent_type, result in results['agent_results'].items():
            if 'error' in result:
                print(f"    ❌ {agent_type}: {result['error']}")
            else:
                print(f"    ✅ {agent_type}: Analysis completed")
                # Show if data was found
                if 'data' in result and 'summary' in result['data']:
                    if 'error' in result['data']['summary']:
                        print(f"      ⚠️  {agent_type}: {result['data']['summary']['error']}")
                    else:
                        print(f"      📊 {agent_type}: Data loaded successfully")
                        # Show data summary
                        summary = result['data']['summary']
                        if 'total_days' in summary:
                            print(f"        📅 Days: {summary['total_days']}")
                        if 'total_locations' in summary:
                            print(f"        🌍 Locations: {summary['total_locations']}")
                        if 'total_revenue_records' in summary:
                            print(f"        💰 Revenue Records: {summary['total_revenue_records']}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error running analysis: {e}")
        return False

def main():
    """Run the test."""
    print("🚀 Starting Agentic Framework Test with Fresh 200k Data\n")
    
    success = test_with_fresh_200k_data()
    
    if success:
        print("\n🎉 Test completed successfully!")
        print("📄 Check the generated markdown report to see insights from 200k aggregation data")
        return 0
    else:
        print("\n❌ Test failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
