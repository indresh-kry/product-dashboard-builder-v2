#!/usr/bin/env python3
"""
Test Script for Agentic Framework
Version: 2.0.0
Last Updated: 2025-10-23

Test script to validate the agentic framework implementation.
"""

import os
import sys
import json
from pathlib import Path

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from agents.agentic_coordinator import AgenticCoordinator
from agents.agent_registry import AgentRegistry

def test_registry():
    """Test the agent registry."""
    print("🧪 Testing Agent Registry...")
    
    registry = AgentRegistry()
    
    # Test getting enabled agents
    enabled_agents = registry.get_enabled_agents()
    print(f"  ✅ Enabled agents: {enabled_agents}")
    
    # Test getting agent summary
    summary = registry.get_agent_summary()
    print(f"  ✅ Registry summary: {summary}")
    
    # Test creating an agent
    try:
        agent = registry.create_agent("daily_metrics", "test_run")
        if agent:
            print(f"  ✅ Successfully created daily_metrics agent")
        else:
            print(f"  ❌ Failed to create daily_metrics agent")
    except Exception as e:
        print(f"  ❌ Error creating agent: {e}")
    
    return True

def test_coordinator():
    """Test the agentic coordinator."""
    print("\n🧪 Testing Agentic Coordinator...")
    
    try:
        coordinator = AgenticCoordinator()
        
        # Test getting registry summary
        summary = coordinator.get_registry_summary()
        print(f"  ✅ Coordinator registry summary: {summary}")
        
        # Test getting agent status
        status = coordinator.get_agent_status("daily_metrics")
        print(f"  ✅ Daily metrics agent status: {status}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error testing coordinator: {e}")
        return False

def test_data_loader():
    """Test the daily metrics data loader."""
    print("\n🧪 Testing Daily Metrics Data Loader...")
    
    try:
        from agents.data_loaders.daily_metrics_loader import DailyMetricsDataLoader
        
        # Test with a known run hash
        test_run_hash = "a83af5"  # Use the existing run
        
        loader = DailyMetricsDataLoader(test_run_hash)
        
        # Test loading data
        data = loader.load_data()
        print(f"  ✅ Data loader returned: {type(data)}")
        
        # Test getting summary
        summary = loader.get_summary()
        print(f"  ✅ Data loader summary: {summary}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error testing data loader: {e}")
        return False

def test_prompt_generator():
    """Test the daily metrics prompt generator."""
    print("\n🧪 Testing Daily Metrics Prompt Generator...")
    
    try:
        from agents.prompt_generators.daily_metrics_generator import DailyMetricsPromptGenerator
        
        generator = DailyMetricsPromptGenerator()
        
        # Test getting system prompt
        system_prompt = generator.get_system_prompt()
        print(f"  ✅ System prompt length: {len(system_prompt)}")
        
        # Test generating prompt
        test_data = {"daily_metrics": "test_data"}
        test_metadata = {"app_filter": "test_app", "date_start": "2025-01-01", "date_end": "2025-01-31"}
        
        prompt = generator.generate_prompt(test_data, test_metadata)
        print(f"  ✅ Generated prompt length: {len(prompt)}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error testing prompt generator: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting Agentic Framework Tests\n")
    
    tests = [
        test_registry,
        test_coordinator,
        test_data_loader,
        test_prompt_generator
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print(f"\n📊 Test Results:")
    print(f"  Passed: {passed}/{total}")
    print(f"  Failed: {total - passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
