#!/usr/bin/env python3
"""
Test Agentic Integration
Version: 2.0.0
Last Updated: 2025-10-23

Test script to validate the agentic framework with real data.
"""

import os
import sys
import json
from pathlib import Path

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from agents.agentic_coordinator import AgenticCoordinator

def test_with_real_data():
    """Test the agentic coordinator with real data."""
    print("🧪 Testing Agentic Coordinator with Real Data...")
    
    # Use the existing run hash
    run_hash = "a83af5"
    
    # Set environment
    os.environ['RUN_HASH'] = run_hash
    
    # Create coordinator
    coordinator = AgenticCoordinator()
    
    # Prepare metadata
    run_metadata = {
        'app_filter': 'com.nukebox.mandir',
        'date_start': '2025-09-15',
        'date_end': '2025-09-30',
        'timestamp': '2025-10-23T10:00:00Z'
    }
    
    print(f"  📊 Run Hash: {run_hash}")
    print(f"  📱 App Filter: {run_metadata['app_filter']}")
    print(f"  📅 Date Range: {run_metadata['date_start']} to {run_metadata['date_end']}")
    
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
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error running analysis: {e}")
        return False

def test_individual_agents():
    """Test individual agents."""
    print("\n🧪 Testing Individual Agents...")
    
    run_hash = "a83af5"
    os.environ['RUN_HASH'] = run_hash
    
    coordinator = AgenticCoordinator()
    
    # Test each agent type
    agent_types = ['daily_metrics', 'user_segmentation', 'geographic', 'cohort_retention', 'revenue_optimization', 'data_quality']
    
    for agent_type in agent_types:
        try:
            status = coordinator.get_agent_status(agent_type)
            print(f"  {agent_type}: {status['status']}")
            
            if status['status'] == 'error':
                print(f"    Error: {status['error']}")
                
        except Exception as e:
            print(f"  ❌ {agent_type}: Error testing agent - {e}")

def main():
    """Run integration tests."""
    print("🚀 Starting Agentic Integration Tests\n")
    
    # Test individual agents
    test_individual_agents()
    
    # Test with real data
    success = test_with_real_data()
    
    if success:
        print("\n🎉 Integration tests completed successfully!")
        return 0
    else:
        print("\n❌ Integration tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
