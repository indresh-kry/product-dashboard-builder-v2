#!/usr/bin/env python3
"""
Test Agentic Framework with Mock Insights
Version: 2.0.0
Last Updated: 2025-10-23

Test script to demonstrate the markdown report with mock LLM responses.
"""

import os
import sys
import json
from pathlib import Path

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from agents.agentic_coordinator import AgenticCoordinator

def create_mock_llm_response():
    """Create a mock LLM response for testing."""
    return {
        "raw_response": "Mock LLM response",
        "parsed_response": {
            "analysis_type": "daily_metrics",
            "summary": "Daily metrics show strong growth with 64% increase in DAU over the period, driven primarily by new user acquisition through Android channels.",
            "trends": {
                "overall_trend": "Positive growth trajectory",
                "key_metrics": ["DAU increased 64%", "Revenue per user improved 23%", "New user acquisition peaked on 2025-09-17"],
                "anomalies": ["Revenue spike on 2025-09-15 ($103.64)", "DAU drop on 2025-09-26 (2,137 users)"]
            },
            "insights": [
                {
                    "metric": "DAU Growth",
                    "finding": "Daily Active Users increased from 1,774 to 2,719 (53% growth) over the 16-day period",
                    "impact": "High",
                    "recommendation": "Continue current acquisition strategy focusing on Android channels",
                    "evidence": "DAU grew from 1,774 (2025-09-15) to 2,719 (2025-09-30), with peak of 3,457 on 2025-09-17"
                },
                {
                    "metric": "Revenue Optimization",
                    "finding": "Revenue per user decreased from $0.058 to $0.006, indicating potential monetization challenges",
                    "impact": "High",
                    "recommendation": "Implement targeted monetization strategies for new users",
                    "evidence": "Revenue per user dropped from $0.058 (2025-09-15) to $0.006 (2025-09-30), with total revenue declining from $103.64 to $17.12"
                }
            ],
            "recommendations": [
                {
                    "category": "User Acquisition",
                    "priority": "High",
                    "action": "Optimize Android channel acquisition based on 2025-09-17 peak performance",
                    "expected_impact": "Increase DAU by 15-20% within 30 days",
                    "evidence": "Android channel drove 2,139 new logins on peak day (2025-09-17) vs average of 1,200"
                },
                {
                    "category": "Monetization",
                    "priority": "High",
                    "action": "Implement onboarding revenue optimization for new users",
                    "expected_impact": "Increase revenue per user by 25% within 60 days",
                    "evidence": "New users show 0% revenue conversion vs 64% for returning users"
                }
            ],
            "data_quality": {
                "completeness": "Excellent - 16 days of complete data with no missing values",
                "consistency": "Good - consistent data structure across all metrics",
                "issues": ["Revenue data shows some outliers on 2025-09-15"]
            },
            "metadata": "High confidence in analysis due to complete 16-day dataset. Recommendations based on clear trend patterns and statistical significance."
        },
        "success": True
    }

def test_with_mock_insights():
    """Test the agentic framework with mock insights."""
    print("🧪 Testing Agentic Framework with Mock Insights...")
    
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
        'timestamp': '2025-10-23T18:30:00Z'
    }
    
    print(f"  📊 Run Hash: {run_hash}")
    print(f"  📱 App Filter: {run_metadata['app_filter']}")
    print(f"  📅 Date Range: {run_metadata['date_start']} to {run_metadata['date_end']}")
    
    # Run analysis
    try:
        results = coordinator.run_analysis(run_hash, run_metadata)
        
        # Inject mock LLM response for daily_metrics agent
        if 'agent_results' in results and 'daily_metrics' in results['agent_results']:
            results['agent_results']['daily_metrics']['llm_response'] = create_mock_llm_response()
            results['summary']['successful_agents'] += 1
            results['summary']['failed_agents'] -= 1
        
        # Regenerate markdown report with mock data
        coordinator._generate_markdown_report(run_hash, results)
        
        print(f"  ✅ Analysis completed successfully")
        print(f"  📈 Agents processed: {len(results['agents_processed'])}")
        print(f"  ✅ Successful agents: {results['summary']['successful_agents']}")
        print(f"  ❌ Failed agents: {results['summary']['failed_agents']}")
        print(f"  🚨 Errors: {results['summary']['errors']}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error running analysis: {e}")
        return False

def main():
    """Run the test."""
    print("🚀 Starting Agentic Framework Test with Mock Insights\n")
    
    success = test_with_mock_insights()
    
    if success:
        print("\n🎉 Test completed successfully!")
        print("📄 Check the generated markdown report to see how insights would be presented")
        return 0
    else:
        print("\n❌ Test failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
