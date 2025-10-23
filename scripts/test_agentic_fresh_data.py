#!/usr/bin/env python3
"""
Test Agentic Framework with Fresh Data
Version: 2.0.0
Last Updated: 2025-10-23

Test script to validate the agentic framework with fresh data from run 981793.
"""

import os
import sys
import json
from pathlib import Path

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from agents.agentic_coordinator import AgenticCoordinator

def test_with_fresh_data():
    """Test the agentic coordinator with fresh data from run 981793."""
    print("🧪 Testing Agentic Framework with Fresh Data from Run 981793...")
    
    # Use the fresh run hash
    run_hash = "981793"
    
    # Set environment
    os.environ['RUN_HASH'] = run_hash
    
    # Create coordinator
    coordinator = AgenticCoordinator()
    
    # Prepare metadata
    run_metadata = {
        'app_filter': 'com.nukebox.mandir',
        'date_start': '2025-09-15',
        'date_end': '2025-09-30',
        'timestamp': '2025-10-23T15:30:00Z'
    }
    
    print(f"  📊 Run Hash: {run_hash}")
    print(f"  📱 App Filter: {run_metadata['app_filter']}")
    print(f"  📅 Date Range: {run_metadata['date_start']} to {run_metadata['date_end']}")
    
    # Check if data exists
    data_dir = Path(f"run_logs/{run_hash}/outputs")
    if not data_dir.exists():
        print(f"  ❌ Data directory not found: {data_dir}")
        return False
    
    print(f"  ✅ Data directory exists: {data_dir}")
    
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
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error running analysis: {e}")
        return False

def main():
    """Run the test."""
    print("🚀 Starting Agentic Framework Test with Fresh Data\n")
    
    success = test_with_fresh_data()
    
    if success:
        print("\n🎉 Test completed successfully!")
        return 0
    else:
        print("\n❌ Test failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
