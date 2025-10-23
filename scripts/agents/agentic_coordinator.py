#!/usr/bin/env python3
"""
Agentic Coordinator
Version: 2.0.0
Last Updated: 2025-10-23

Main coordinator for the agentic LLM framework.
Orchestrates multiple agents and coordinates their analysis.
"""

import os
import json
import sys
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

from .agent_registry import AgentRegistry
from .base_agent import BaseAgent, LLMAgent

class AgenticCoordinator:
    """Main coordinator for agentic analysis."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.registry = AgentRegistry(config_path)
        self.run_hash = os.environ.get('RUN_HASH', 'unknown')
        self.results = {}
        
    def run_analysis(self, run_hash: str, run_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Run analysis with all enabled agents."""
        print(f"🚀 Starting agentic analysis for run: {run_hash}", file=sys.stderr)
        
        # Get enabled agents
        enabled_agents = self.registry.get_enabled_agents()
        print(f"📋 Enabled agents: {enabled_agents}", file=sys.stderr)
        
        # Sort by priority
        enabled_agents.sort(key=lambda x: self.registry.get_agent_priority(x))
        
        results = {
            'run_hash': run_hash,
            'timestamp': datetime.now().isoformat(),
            'agents_processed': [],
            'agent_results': {},
            'summary': {},
            'errors': []
        }
        
        # Process each agent
        for agent_type in enabled_agents:
            try:
                print(f"🤖 Processing agent: {agent_type}", file=sys.stderr)
                
                # Create agent
                agent = self.registry.create_agent(agent_type, run_hash)
                if not agent:
                    error_msg = f"Failed to create agent: {agent_type}"
                    print(f"❌ {error_msg}", file=sys.stderr)
                    results['errors'].append(error_msg)
                    continue
                
                # Run analysis
                if isinstance(agent, LLMAgent):
                    agent_result = agent.analyze_with_llm(run_hash, run_metadata)
                else:
                    agent_result = agent.analyze(run_hash, run_metadata)
                
                # Store result
                results['agent_results'][agent_type] = agent_result
                results['agents_processed'].append(agent_type)
                
                print(f"✅ Agent {agent_type} completed", file=sys.stderr)
                
            except Exception as e:
                error_msg = f"Error processing agent {agent_type}: {str(e)}"
                print(f"❌ {error_msg}", file=sys.stderr)
                results['errors'].append(error_msg)
        
        # Generate summary
        results['summary'] = self._generate_summary(results)
        
        # Save results
        self._save_results(run_hash, results)
        
        # Generate human-readable markdown report
        self._generate_markdown_report(run_hash, results)
        
        print(f"🎯 Analysis completed. Processed {len(results['agents_processed'])} agents", file=sys.stderr)
        
        return results
    
    def _generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary of analysis results."""
        summary = {
            'total_agents': len(results['agents_processed']),
            'successful_agents': len([r for r in results['agent_results'].values() if 'error' not in r]),
            'failed_agents': len([r for r in results['agent_results'].values() if 'error' in r]),
            'errors': len(results['errors']),
            'agent_types': results['agents_processed']
        }
        
        return summary
    
    def _save_results(self, run_hash: str, results: Dict[str, Any]):
        """Save analysis results to file."""
        try:
            output_dir = Path(f"run_logs/{run_hash}/outputs/insights")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save agentic results
            agentic_output_path = output_dir / "agentic_insights.json"
            with open(agentic_output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"💾 Results saved to: {agentic_output_path}", file=sys.stderr)
            
        except Exception as e:
            print(f"⚠️ Error saving results: {e}", file=sys.stderr)
    
    def _generate_markdown_report(self, run_hash: str, results: Dict[str, Any]):
        """Generate human-readable markdown report from agent results."""
        try:
            output_dir = Path(f"run_logs/{run_hash}/outputs/insights")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate markdown content
            markdown_content = self._create_markdown_content(results)
            
            # Save markdown report
            markdown_path = output_dir / "agentic_insights_report.md"
            with open(markdown_path, 'w') as f:
                f.write(markdown_content)
            
            print(f"📄 Markdown report saved to: {markdown_path}", file=sys.stderr)
            
        except Exception as e:
            print(f"⚠️ Error generating markdown report: {e}", file=sys.stderr)
    
    def _create_markdown_content(self, results: Dict[str, Any]) -> str:
        """Create markdown content from agent results."""
        content = []
        
        # Header
        content.append("# Agentic Insights Report")
        content.append("")
        content.append(f"**Run Hash:** {results['run_hash']}")
        content.append(f"**Generated:** {results['timestamp']}")
        content.append(f"**Agents Processed:** {len(results['agents_processed'])}")
        content.append("")
        
        # Summary
        summary = results['summary']
        content.append("## Executive Summary")
        content.append("")
        content.append(f"- **Total Agents:** {summary['total_agents']}")
        content.append(f"- **Successful:** {summary['successful_agents']}")
        content.append(f"- **Failed:** {summary['failed_agents']}")
        content.append(f"- **Errors:** {summary['errors']}")
        content.append("")
        
        # Agent Results
        content.append("## Agent Analysis Results")
        content.append("")
        
        for agent_type in results['agents_processed']:
            agent_result = results['agent_results'].get(agent_type, {})
            
            content.append(f"### {agent_type.replace('_', ' ').title()}")
            content.append("")
            
            if 'error' in agent_result:
                content.append(f"❌ **Error:** {agent_result['error']}")
                content.append("")
            else:
                # Data summary
                if 'data' in agent_result and 'summary' in agent_result['data']:
                    data_summary = agent_result['data']['summary']
                    if 'error' in data_summary:
                        content.append(f"⚠️ **Data Status:** {data_summary['error']}")
                    else:
                        content.append("✅ **Data Status:** Data loaded successfully")
                        if 'total_days' in data_summary:
                            content.append(f"- **Days Analyzed:** {data_summary['total_days']}")
                        if 'date_range' in data_summary:
                            date_range = data_summary['date_range']
                            content.append(f"- **Date Range:** {date_range.get('start', 'N/A')} to {date_range.get('end', 'N/A')}")
                        if 'metrics_available' in data_summary:
                            content.append(f"- **Metrics Available:** {', '.join(data_summary['metrics_available'])}")
                content.append("")
                
                # LLM Response
                if 'llm_response' in agent_result:
                    llm_response = agent_result['llm_response']
                    if llm_response.get('success'):
                        content.append("🤖 **LLM Analysis:** Completed successfully")
                        if 'parsed_response' in llm_response and llm_response['parsed_response']:
                            parsed = llm_response['parsed_response']
                            
                            # Summary
                            if 'summary' in parsed:
                                content.append(f"**Summary:** {parsed['summary']}")
                                content.append("")
                            
                            # Insights
                            if 'insights' in parsed and parsed['insights']:
                                content.append("**Key Insights:**")
                                content.append("")
                                for insight in parsed['insights']:
                                    # Get the appropriate key for the insight type
                                    insight_key = insight.get('metric') or insight.get('segment') or insight.get('region') or insight.get('cohort') or insight.get('issue') or 'Unknown'
                                    content.append(f"- **{insight_key}:** {insight.get('finding', 'N/A')}")
                                    if 'evidence' in insight:
                                        content.append(f"  - *Evidence:* {insight['evidence']}")
                                    if 'recommendation' in insight:
                                        content.append(f"  - *Recommendation:* {insight['recommendation']}")
                                content.append("")
                            
                            # Recommendations
                            if 'recommendations' in parsed and parsed['recommendations']:
                                content.append("**Recommendations:**")
                                content.append("")
                                for rec in parsed['recommendations']:
                                    content.append(f"- **{rec.get('category', 'General')}:** {rec.get('action', 'N/A')}")
                                    if 'priority' in rec:
                                        content.append(f"  - *Priority:* {rec['priority']}")
                                    if 'expected_impact' in rec:
                                        content.append(f"  - *Expected Impact:* {rec['expected_impact']}")
                                    if 'evidence' in rec:
                                        content.append(f"  - *Evidence:* {rec['evidence']}")
                                content.append("")
                            
                            # Data Quality
                            if 'data_quality' in parsed and parsed['data_quality']:
                                content.append("**Data Quality Assessment:**")
                                content.append("")
                                dq = parsed['data_quality']
                                if 'completeness' in dq:
                                    content.append(f"- **Completeness:** {dq['completeness']}")
                                if 'consistency' in dq:
                                    content.append(f"- **Consistency:** {dq['consistency']}")
                                if 'issues' in dq and dq['issues']:
                                    content.append("- **Issues:**")
                                    for issue in dq['issues']:
                                        content.append(f"  - {issue}")
                                content.append("")
                            
                            # Metadata
                            if 'metadata' in parsed:
                                content.append(f"**Additional Notes:** {parsed['metadata']}")
                                content.append("")
                    else:
                        content.append("❌ **LLM Analysis:** Failed")
                        if 'error' in llm_response:
                            content.append(f"**Error:** {llm_response['error']}")
                        content.append("")
                else:
                    content.append("⚠️ **LLM Analysis:** Not available")
                    content.append("")
        
        # Errors section
        if results['errors']:
            content.append("## Errors and Issues")
            content.append("")
            for error in results['errors']:
                content.append(f"- ❌ {error}")
            content.append("")
        
        # Footer
        content.append("---")
        content.append("")
        content.append("*This report was generated by the Agentic LLM Framework v2.0.0*")
        content.append("")
        content.append("*For technical details, see the corresponding JSON files in the insights directory.*")
        
        return "\n".join(content)
    
    def get_agent_status(self, agent_type: str) -> Dict[str, Any]:
        """Get status of a specific agent."""
        if agent_type not in self.registry.get_enabled_agents():
            return {'status': 'disabled', 'error': 'Agent not enabled'}
        
        try:
            agent = self.registry.create_agent(agent_type, self.run_hash)
            if not agent:
                return {'status': 'error', 'error': 'Failed to create agent'}
            
            return {
                'status': 'available',
                'agent_type': agent_type,
                'llm_enabled': isinstance(agent, LLMAgent),
                'priority': self.registry.get_agent_priority(agent_type)
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def get_registry_summary(self) -> Dict[str, Any]:
        """Get summary of agent registry."""
        return self.registry.get_agent_summary()

def main():
    """Main function for testing the coordinator."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Agentic LLM Coordinator')
    parser.add_argument('--run-hash', required=True, help='Run hash for analysis')
    parser.add_argument('--config', help='Path to agent configuration file')
    parser.add_argument('--app-filter', help='App filter for analysis')
    parser.add_argument('--date-start', help='Start date for analysis')
    parser.add_argument('--date-end', help='End date for analysis')
    
    args = parser.parse_args()
    
    # Set environment variables
    os.environ['RUN_HASH'] = args.run_hash
    
    # Create coordinator
    coordinator = AgenticCoordinator(args.config)
    
    # Prepare metadata
    run_metadata = {
        'app_filter': args.app_filter,
        'date_start': args.date_start,
        'date_end': args.date_end,
        'timestamp': datetime.now().isoformat()
    }
    
    # Run analysis
    results = coordinator.run_analysis(args.run_hash, run_metadata)
    
    # Print summary
    print(f"\n📊 Analysis Summary:", file=sys.stderr)
    print(f"  Agents Processed: {results['summary']['total_agents']}", file=sys.stderr)
    print(f"  Successful: {results['summary']['successful_agents']}", file=sys.stderr)
    print(f"  Failed: {results['summary']['failed_agents']}", file=sys.stderr)
    print(f"  Errors: {results['summary']['errors']}", file=sys.stderr)
    
    if results['errors']:
        print(f"\n❌ Errors:", file=sys.stderr)
        for error in results['errors']:
            print(f"  - {error}", file=sys.stderr)

if __name__ == "__main__":
    main()
