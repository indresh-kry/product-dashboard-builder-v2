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
from .llm_client import LLMClient

class AgenticCoordinator:
    """Main coordinator for agentic analysis."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.registry = AgentRegistry(config_path)
        self.run_hash = os.environ.get('RUN_HASH', 'unknown')
        self.results = {}
        # Initialize LLM client for coordinator synthesis
        try:
            self.llm_client = LLMClient()
        except Exception as e:
            print(f"⚠️ Warning: Could not initialize LLM client for coordinator: {e}", file=sys.stderr)
            self.llm_client = None
        
    def run_analysis(self, run_hash: str, run_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Run analysis with all enabled agents."""
        # Guard: only allow when orchestrator explicitly enables Phase 5
        if os.environ.get('ALLOW_AGENTIC_NOW') != '1' and os.environ.get('ORCHESTRATOR_PHASE') != '5':
            return {
                'run_hash': run_hash,
                'timestamp': datetime.now().isoformat(),
                'agents_processed': [],
                'agent_results': {},
                'summary': {'total_agents': 0, 'successful_agents': 0, 'failed_agents': 0, 'errors': 0},
                'errors': ['Agentic analysis skipped: not in orchestrator Phase 5']
            }

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
        
        # Store run metadata for business metrics calculation
        results['run_metadata'] = run_metadata
        
        # Phase 2: Coordinator LLM Synthesis
        if self.llm_client and self.llm_client.is_available():
            print(f"🧠 Running Coordinator LLM synthesis...", file=sys.stderr)
            try:
                coordinator_synthesis = self._synthesize_with_coordinator_llm(results)
                results['coordinator_synthesis'] = coordinator_synthesis
                print(f"✅ Coordinator synthesis completed", file=sys.stderr)
            except Exception as e:
                error_msg = f"Coordinator LLM synthesis failed: {str(e)}"
                print(f"⚠️ {error_msg}", file=sys.stderr)
                results['errors'].append(error_msg)
        else:
            print(f"⚠️ Coordinator LLM not available, skipping synthesis", file=sys.stderr)
        
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
    
    def _synthesize_with_coordinator_llm(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Phase 2: Synthesize insights from all child agents using Coordinator LLM."""
        # Collect all child agent insights and recommendations
        child_insights = {}
        all_recommendations = []
        
        agent_descriptions = {
            'daily_metrics': 'Daily Metrics Analyst - trends, anomalies, daily patterns',
            'user_segmentation': 'User Segmentation Analyst - user behavior, segments, churn',
            'geographic': 'Geographic Analyst - regional performance',
            'cohort_retention': 'Cohort Retention Analyst - lifecycle, retention patterns',
            'revenue_optimization': 'Revenue Optimization Analyst - revenue streams, monetization',
            'data_quality': 'Data Quality Analyst - data integrity, completeness'
        }
        
        for agent_type in results['agents_processed']:
            agent_result = results['agent_results'].get(agent_type, {})
            
            if 'error' not in agent_result and 'llm_response' in agent_result:
                llm_response = agent_result['llm_response']
                if llm_response.get('success') and 'parsed_response' in llm_response and llm_response['parsed_response']:
                    parsed = llm_response['parsed_response']
                    
                    # Collect insights
                    insights = parsed.get('insights', [])
                    recommendations = parsed.get('recommendations', [])
                    
                    if insights or recommendations:
                        # Extract temporal information from insights for cross-agent correlation
                        enhanced_insights = []
                        for insight in insights:
                            enhanced_insight = insight.copy()
                            # Try to extract date/time from insight fields
                            insight_text = str(insight.get('finding', '')) + ' ' + str(insight.get('evidence', ''))
                            # Look for date patterns (YYYY-MM-DD, MM/DD/YYYY, etc.)
                            import re
                            date_patterns = [
                                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                                r'\d{2}/\d{2}/\d{4}',   # MM/DD/YYYY
                                r'day\s+\d+',           # "day 5", "day 10"
                                r'cohort\s+\d{4}-\d{2}-\d{2}',  # "cohort 2025-08-20"
                            ]
                            dates_found = []
                            for pattern in date_patterns:
                                matches = re.findall(pattern, insight_text, re.IGNORECASE)
                                dates_found.extend(matches)
                            if dates_found:
                                enhanced_insight['temporal_markers'] = list(set(dates_found))
                            enhanced_insights.append(enhanced_insight)
                        
                        child_insights[agent_type] = {
                            'agent_description': agent_descriptions.get(agent_type, agent_type),
                            'summary': parsed.get('summary', ''),
                            'insights': enhanced_insights,
                            'recommendations': recommendations,
                            'data_quality': parsed.get('data_quality', {}),
                            'date_range': agent_result.get('data', {}).get('summary', {}).get('date_range', {})
                        }
                        
                        # Add agent type to each recommendation for tracking
                        for rec in recommendations:
                            rec_with_agent = rec.copy()
                            rec_with_agent['source_agent'] = agent_type
                            all_recommendations.append(rec_with_agent)
        
        # Build synthesis prompt
        synthesis_prompt = self._build_synthesis_prompt(child_insights, all_recommendations)
        system_prompt = self._get_coordinator_system_prompt()
        
        # Call LLM for synthesis
        llm_response = self.llm_client.call(
            prompt=synthesis_prompt,
            system_prompt=system_prompt,
            temperature=0.3,
            max_tokens=4000
        )
        
        if llm_response.get('success') and llm_response.get('parsed_response'):
            return {
                'llm_response': llm_response,
                'synthesis': llm_response['parsed_response'],
                'child_agents_count': len(child_insights),
                'total_recommendations_analyzed': len(all_recommendations)
            }
        else:
            return {
                'error': llm_response.get('error', 'Unknown error'),
                'raw_response': llm_response.get('raw_response'),
                'child_agents_count': len(child_insights),
                'total_recommendations_analyzed': len(all_recommendations)
            }
    
    def _get_coordinator_system_prompt(self) -> str:
        """Get system prompt for Coordinator LLM synthesis."""
        return """You are an executive product strategy consultant synthesizing insights from 6 specialized analysts:

1. Daily Metrics Analyst - trends, anomalies, daily patterns
2. User Segmentation Analyst - user behavior, segments, churn
3. Geographic Analyst - regional performance
4. Cohort Retention Analyst - lifecycle, retention patterns
5. Revenue Optimization Analyst - revenue streams, monetization
6. Data Quality Analyst - data integrity, completeness

YOUR TASK:
1. Identify the TOP 3 most impactful insights across all analysts (with summary, evidence, and related data)
2. Identify the TOP 3 most impactful recommendations across all analysts (with evidence and related numbers)
3. Resolve conflicts (e.g., if one says "focus on IAP" and another says "focus on ads")
4. Rank recommendations by: Expected Impact × Confidence × Implementation Feasibility
5. **CRITICAL: Find cross-agent correlations and causal relationships:**
   - Connect insights across agents (e.g., "Revenue dropped on Day X because engagement was low")
   - Identify root causes by linking findings (e.g., "Low retention explains revenue decline")
   - Find correlations between different metrics (e.g., "New player engagement down → Revenue down")
   - Build causal chains: "Agent A found X → Agent B found Y → This explains Z"
   - Look for temporal alignment (same dates/time periods across agents)
5. Create executive summary highlighting:
   - Most critical issue requiring immediate action
   - Highest ROI opportunity
   - Cross-agent insights and correlations discovered
   - Data quality concerns affecting decisions
   - Missing data needed for better insights
6. Identify overlapping/redundant recommendations and merge them
7. Flag any contradictions between agents

OUTPUT FORMAT (JSON):
{
  "executive_summary": "2-3 paragraph executive summary of key findings",
  "top_3_prioritized_recommendations": [
    {
      "rank": 1,
      "category": "Category name",
      "action": "Specific actionable recommendation",
      "priority": "High/Medium/Low",
      "expected_impact": "Description of expected impact",
      "timeframe": "Specific timeframe (days/weeks)",
      "confidence": "High/Medium/Low",
      "source_agents": ["agent1", "agent2"],
      "conflict_resolution": "If multiple agents had conflicting recommendations, explain resolution",
      "implementation_complexity": "Simple/Medium/Complex",
      "evidence": "Specific data points, numbers, and metrics supporting this recommendation",
      "related_numbers": "Key metrics, percentages, dollar amounts, user counts, etc. that support this recommendation"
    }
  ],
  "top_3_insights": [
    {
      "rank": 1,
      "insight": "Key insight or finding",
      "summary": "Brief summary of the insight",
      "evidence": "Specific data points, numbers, dates supporting this insight",
      "related_data": "Key metrics, percentages, trends, comparisons",
      "source_agents": ["agent1", "agent2"],
      "relevance": "How this insight relates to business goals or recommendations (or 'standalone' if unrelated)"
    }
  ],
  "conflicts_resolved": [
    {
      "conflict_description": "Description of the conflict",
      "resolution": "How you resolved it",
      "agents_involved": ["agent1", "agent2"]
    }
  ],
  "data_gaps": [
    "List of missing data needed for better insights"
  ],
  "risk_assessments": [
    {
      "risk": "Description of risk",
      "severity": "High/Medium/Low",
      "mitigation": "Suggested mitigation"
    }
  ],
  "key_insights_synthesis": [
    {
      "insight": "Synthesized insight from multiple agents",
      "supporting_agents": ["agent1", "agent2"],
      "evidence": "Supporting evidence"
    }
  ],
  "cross_agent_correlations": [
    {
      "correlation_type": "causal/correlational/temporal",
      "primary_insight": "Insight from agent A (e.g., 'Revenue dropped 40% on Day X')",
      "correlated_insight": "Insight from agent B (e.g., 'New player engagement down 35% on Day X')",
      "relationship": "How they relate (e.g., 'Low engagement of new players likely caused revenue drop')",
      "agents_involved": ["revenue_optimization", "daily_metrics"],
      "confidence": "High/Medium/Low",
      "evidence": "Supporting data points and dates",
      "implication": "What this means for action (e.g., 'Focus on new player onboarding to improve revenue')"
    }
  ],
  "causal_chains": [
    {
      "chain": [
        {"agent": "agent1", "finding": "Finding 1"},
        {"agent": "agent2", "finding": "Finding 2"},
        {"agent": "agent3", "finding": "Finding 3"}
      ],
      "root_cause": "The root cause identified",
      "final_effect": "The final business impact",
      "recommended_action": "Action to address root cause"
    }
  ]
}

CRITICAL REQUIREMENTS:
- Recommendations must be specific and actionable (WHO, WHAT, WHEN, EXPECTED OUTCOME)
- **MUST identify cross-agent correlations**: Look for patterns where insights from different agents explain each other
- **MUST identify causal relationships**: Connect findings across agents (e.g., engagement → revenue, retention → monetization)
- **MUST check temporal alignment**: If revenue drops on Day X, check if engagement/retention also dropped on Day X
- **MUST build causal chains**: Trace root causes through multiple agents (e.g., Data Quality → Segmentation → Revenue)
- Prioritize recommendations that have data support from multiple agents
- Resolve conflicts by considering data quality, impact, and feasibility
- Executive summary should be concise (2-3 paragraphs) and highlight most critical actions and cross-agent findings"""
    
    def _build_synthesis_prompt(self, child_insights: Dict[str, Any], all_recommendations: List[Dict[str, Any]]) -> str:
        """Build prompt for coordinator synthesis from child agent results."""
        prompt_parts = []
        
        prompt_parts.append("## CHILD AGENT ANALYSIS RESULTS")
        prompt_parts.append("")
        prompt_parts.append(f"Below are the insights and recommendations from {len(child_insights)} specialized analysts:")
        prompt_parts.append("")
        
        # Add each agent's results
        for agent_type, agent_data in child_insights.items():
            prompt_parts.append(f"### {agent_data['agent_description']}")
            prompt_parts.append("")
            
            if agent_data.get('summary'):
                prompt_parts.append(f"**Summary:** {agent_data['summary']}")
                prompt_parts.append("")
            
            if agent_data.get('insights'):
                prompt_parts.append("**Key Insights:**")
                for insight in agent_data['insights'][:5]:  # Limit to top 5 per agent
                    insight_text = f"- {insight.get('finding', 'N/A')}"
                    if insight.get('evidence'):
                        insight_text += f" (Evidence: {insight['evidence']})"
                    # Include temporal markers if available for cross-agent correlation
                    if insight.get('temporal_markers'):
                        insight_text += f" [Dates/Times: {', '.join(insight['temporal_markers'])}]"
                    prompt_parts.append(insight_text)
                prompt_parts.append("")
            
            # Include date range information for temporal correlation
            if agent_data.get('date_range'):
                date_range = agent_data['date_range']
                if date_range.get('start') or date_range.get('end'):
                    prompt_parts.append(f"**Analysis Date Range:** {date_range.get('start', 'N/A')} to {date_range.get('end', 'N/A')}")
                    prompt_parts.append("")
            
            if agent_data.get('recommendations'):
                prompt_parts.append("**Recommendations:**")
                for rec in agent_data['recommendations'][:5]:  # Limit to top 5 per agent
                    rec_text = f"- **{rec.get('category', 'General')}** [{rec.get('priority', 'Medium')}]: {rec.get('action', 'N/A')}"
                    if rec.get('expected_impact'):
                        rec_text += f" | Impact: {rec['expected_impact']}"
                    prompt_parts.append(rec_text)
                prompt_parts.append("")
            
            if agent_data.get('data_quality', {}).get('issues'):
                prompt_parts.append("**Data Quality Issues:**")
                for issue in agent_data['data_quality']['issues'][:3]:  # Limit to top 3
                    prompt_parts.append(f"- {issue}")
                prompt_parts.append("")
        
        prompt_parts.append("## SYNTHESIS TASK")
        prompt_parts.append("")
        prompt_parts.append("Based on the above analysis from all agents:")
        prompt_parts.append("1. Identify the TOP 3 most impactful insights (with summary, evidence, and related data)")
        prompt_parts.append("2. Identify the TOP 3 most impactful recommendations (with evidence and related numbers)")
        prompt_parts.append("3. **Find cross-agent correlations and causal relationships:**")
        prompt_parts.append("   - Look for insights that explain each other (e.g., low engagement → low revenue)")
        prompt_parts.append("   - Check if findings from different agents align on the same dates/time periods")
        prompt_parts.append("   - Build causal chains: What agent found X that explains what agent found Y")
        prompt_parts.append("   - Identify root causes by connecting findings across agents")
        prompt_parts.append("   - Example: If Revenue agent found 'Revenue dropped 40% on 2025-08-20' and")
        prompt_parts.append("     Daily Metrics agent found 'New player engagement down 35% on 2025-08-20',")
        prompt_parts.append("     this is a cross-agent correlation showing likely causality")
        prompt_parts.append("4. Resolve any conflicts between agents")
        prompt_parts.append("5. Create an executive summary highlighting cross-agent findings")
        prompt_parts.append("6. Identify data gaps and risks")
        prompt_parts.append("")
        prompt_parts.append("CRITICAL: For recommendations, include:")
        prompt_parts.append("- Specific evidence (data points, dates, metrics)")
        prompt_parts.append("- Related numbers (percentages, dollar amounts, user counts, etc.)")
        prompt_parts.append("- For insights, include summary, evidence, and related data")
        prompt_parts.append("")
        prompt_parts.append("Return your synthesis in the required JSON format, including cross_agent_correlations and causal_chains.")
        
        return "\n".join(prompt_parts)
    
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
            
            # Generate visualizations
            try:
                from .visualization_generator import ReportVisualizationGenerator
                viz_generator = ReportVisualizationGenerator(run_hash)
                charts = viz_generator.generate_all_charts()
                
                # Add visualization section to content
                if charts:
                    viz_section = viz_generator.generate_chart_summary(charts)
                    # Insert visualizations after Key Business Metrics section
                    content_parts = markdown_content.split("## Executive Summary")
                    if len(content_parts) > 1:
                        markdown_content = content_parts[0] + "\n" + viz_section + "\n## Executive Summary" + content_parts[1]
                    else:
                        markdown_content += "\n" + viz_section
                        
            except Exception as viz_error:
                print(f"⚠️ Error generating visualizations: {viz_error}", file=sys.stderr)
            
            # Save markdown report
            markdown_path = output_dir / "agentic_insights_report.md"
            with open(markdown_path, 'w') as f:
                f.write(markdown_content)
            
            print(f"📄 Markdown report saved to: {markdown_path}", file=sys.stderr)
            
        except Exception as e:
            print(f"⚠️ Error generating markdown report: {e}", file=sys.stderr)
    
    def _calculate_business_metrics(self, results: Dict[str, Any], run_metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Calculate key business metrics from agent data."""
        metrics = {}
        
        try:
            # Add app name if available
            if run_metadata and 'app_filter' in run_metadata:
                metrics['app_name'] = run_metadata['app_filter']
            # Get daily metrics data if available
            daily_metrics_data = None
            if 'daily_metrics' in results['agent_results']:
                daily_result = results['agent_results']['daily_metrics']
                if 'data' in daily_result and 'daily_metrics' in daily_result['data']:
                    daily_metrics_data = daily_result['data']['daily_metrics']
            
            if daily_metrics_data is not None and hasattr(daily_metrics_data, 'columns'):
                # Calculate metrics from daily data
                df = daily_metrics_data
                
                # Duration of analysis - use specified date range from run metadata
                if run_metadata and 'start_date' in run_metadata and 'end_date' in run_metadata:
                    metrics['duration'] = f"{run_metadata['start_date']} to {run_metadata['end_date']}"
                elif 'date' in df.columns:
                    # Fallback to actual data range if metadata not available
                    start_date = df['date'].min()
                    end_date = df['date'].max()
                    metrics['duration'] = f"{start_date} to {end_date}"
                
                # Average daily users
                if 'total_dau' in df.columns:
                    metrics['avg_daily_users'] = round(df['total_dau'].mean(), 0)
                
                # Average daily new users
                if 'new_users' in df.columns:
                    metrics['avg_daily_new_users'] = round(df['new_users'].mean(), 0)
                
                # Total revenue
                if 'total_revenue' in df.columns:
                    total_revenue = df['total_revenue'].sum()
                    metrics['total_revenue'] = round(total_revenue, 2)
                    metrics['avg_daily_revenue'] = round(df['total_revenue'].mean(), 2)
                
                # Calculate true D1 retention using days_since_first_event from aggregated data
                try:
                    # Load aggregated data to get true D1 retention
                    import pandas as pd
                    agg_df = pd.read_csv(f"run_logs/{results['run_hash']}/outputs/aggregations/aggregated_data.csv")
                    
                    if 'days_since_first_event' in agg_df.columns and 'date' in agg_df.columns:
                        # Calculate true D1 retention: users with days_since_first_event=0 on day N 
                        # who returned on day N+1 (days_since_first_event=1)
                        d1_retention_rates = []
                        dates = sorted(agg_df['date'].unique())
                        
                        for i in range(len(dates) - 1):
                            current_date = dates[i]
                            next_date = dates[i + 1]
                            
                            # Users who were new on current_date (days_since_first_event = 0)
                            new_users = len(agg_df[(agg_df['date'] == current_date) & (agg_df['days_since_first_event'] == 0)])
                            
                            # Users who returned the next day (days_since_first_event = 1 on next_date)
                            # These are users who were new on current_date and returned on next_date
                            retained_users = len(agg_df[(agg_df['date'] == next_date) & (agg_df['days_since_first_event'] == 1)])
                            
                            if new_users > 0:
                                retention_rate = (retained_users / new_users) * 100
                                d1_retention_rates.append(retention_rate)
                        
                        if d1_retention_rates:
                            metrics['avg_d1_retention'] = round(sum(d1_retention_rates) / len(d1_retention_rates), 1)
                            
                except Exception as e:
                    print(f"⚠️ Error calculating true D1 retention: {e}", file=sys.stderr)
                    # Fallback to returning user percentage if aggregated data not available
                    if 'returning_user_percentage' in df.columns:
                        metrics['avg_d1_retention'] = round(df['returning_user_percentage'].mean(), 1)
                
                # Get whale users count if available
                whale_count = 0
                try:
                    if 'daily_revenue_by_type' in results['agent_results'].get('daily_metrics', {}).get('data', {}):
                        revenue_data = results['agent_results']['daily_metrics']['data']['daily_revenue_by_type']
                        if hasattr(revenue_data, 'columns') and 'revenue_segment' in revenue_data.columns:
                            whale_data = revenue_data[revenue_data['revenue_segment'] == 'whale']
                            if 'revenue_users' in whale_data.columns:
                                whale_count = whale_data['revenue_users'].sum()
                except Exception as e:
                    print(f"⚠️ Error calculating whale users: {e}", file=sys.stderr)
                
                metrics['total_whale_users'] = int(whale_count)
                
        except Exception as e:
            print(f"⚠️ Error calculating business metrics: {e}", file=sys.stderr)
        
        return metrics

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
        
        # Business Metrics
        business_metrics = self._calculate_business_metrics(results, results.get('run_metadata', {}))
        if business_metrics:
            content.append("## Key Business Metrics")
            content.append("")
            if 'app_name' in business_metrics:
                content.append(f"- **App Name:** {business_metrics['app_name']}")
            if 'duration' in business_metrics:
                content.append(f"- **Analysis Duration:** {business_metrics['duration']}")
            if 'avg_daily_users' in business_metrics:
                content.append(f"- **Average Daily Users:** {business_metrics['avg_daily_users']:,.0f}")
            if 'avg_daily_new_users' in business_metrics:
                content.append(f"- **Average Daily New Users:** {business_metrics['avg_daily_new_users']:,.0f}")
            if 'avg_d1_retention' in business_metrics:
                content.append(f"- **Average D1 Retention:** {business_metrics['avg_d1_retention']:.1f}%")
            if 'total_revenue' in business_metrics:
                content.append(f"- **Total Revenue:** ${business_metrics['total_revenue']:,.2f}")
            if 'avg_daily_revenue' in business_metrics:
                content.append(f"- **Average Daily Revenue:** ${business_metrics['avg_daily_revenue']:,.2f}")
            if 'total_whale_users' in business_metrics:
                content.append(f"- **Total Whale Users:** {business_metrics['total_whale_users']:,}")
            content.append("")
        
        # Coordinator Synthesis (Phase 2)
        if 'coordinator_synthesis' in results and results['coordinator_synthesis'].get('synthesis'):
            synthesis = results['coordinator_synthesis']['synthesis']
            content.append("## Executive Summary (Coordinator Synthesis)")
            content.append("")
            
            if synthesis.get('executive_summary'):
                content.append(synthesis['executive_summary'])
                content.append("")
            
            # Top 3 Insights Section
            if synthesis.get('top_3_insights'):
                content.append("### Top 3 Insights")
                content.append("")
                for insight in synthesis['top_3_insights']:
                    content.append(f"**#{insight.get('rank', 'N/A')} - {insight.get('insight', 'N/A')}**")
                    if insight.get('summary'):
                        content.append(f"{insight['summary']}")
                        content.append("")
                    if insight.get('evidence'):
                        content.append(f"*Evidence:* {insight['evidence']}")
                    if insight.get('related_data'):
                        content.append(f"*Related Data:* {insight['related_data']}")
                    if insight.get('source_agents'):
                        content.append(f"*Source Agents:* {', '.join(insight['source_agents'])}")
                    if insight.get('relevance'):
                        content.append(f"*Relevance:* {insight['relevance']}")
                    content.append("")
            
            # Top 3 Recommendations Section
            if synthesis.get('top_3_prioritized_recommendations'):
                content.append("### Top 3 Prioritized Recommendations")
                content.append("")
                for rec in synthesis['top_3_prioritized_recommendations']:
                    priority_emoji = "🔴" if rec.get('priority') == 'High' else "🟡" if rec.get('priority') == 'Medium' else "🟢"
                    content.append(f"{priority_emoji} **#{rec.get('rank', 'N/A')} - {rec.get('category', 'General')} [{rec.get('priority', 'Medium')}]**")
                    content.append(f"{rec.get('action', 'N/A')}")
                    if rec.get('evidence'):
                        content.append(f"*Evidence:* {rec['evidence']}")
                    if rec.get('related_numbers'):
                        content.append(f"*Related Numbers:* {rec['related_numbers']}")
                    if rec.get('expected_impact'):
                        content.append(f"*Expected Impact:* {rec['expected_impact']}")
                    if rec.get('timeframe'):
                        content.append(f"*Timeframe:* {rec['timeframe']}")
                    if rec.get('source_agents'):
                        content.append(f"*Source Agents:* {', '.join(rec['source_agents'])}")
                    if rec.get('conflict_resolution'):
                        content.append(f"*Conflict Resolution:* {rec['conflict_resolution']}")
                    content.append("")
            
            # Fallback for old format (top_5_prioritized_recommendations)
            elif synthesis.get('top_5_prioritized_recommendations'):
                content.append("### Top 3 Prioritized Recommendations")
                content.append("")
                for rec in synthesis['top_5_prioritized_recommendations'][:3]:  # Limit to top 3
                    priority_emoji = "🔴" if rec.get('priority') == 'High' else "🟡" if rec.get('priority') == 'Medium' else "🟢"
                    content.append(f"{priority_emoji} **#{rec.get('rank', 'N/A')} - {rec.get('category', 'General')} [{rec.get('priority', 'Medium')}]**")
                    content.append(f"{rec.get('action', 'N/A')}")
                    if rec.get('evidence'):
                        content.append(f"*Evidence:* {rec['evidence']}")
                    if rec.get('related_numbers'):
                        content.append(f"*Related Numbers:* {rec['related_numbers']}")
                    if rec.get('expected_impact'):
                        content.append(f"*Expected Impact:* {rec['expected_impact']}")
                    if rec.get('timeframe'):
                        content.append(f"*Timeframe:* {rec['timeframe']}")
                    if rec.get('source_agents'):
                        content.append(f"*Source Agents:* {', '.join(rec['source_agents'])}")
                    content.append("")
            
            if synthesis.get('conflicts_resolved'):
                content.append("### Conflicts Resolved")
                content.append("")
                for conflict in synthesis['conflicts_resolved']:
                    content.append(f"- **Conflict:** {conflict.get('conflict_description', 'N/A')}")
                    content.append(f"  - *Resolution:* {conflict.get('resolution', 'N/A')}")
                    content.append(f"  - *Agents Involved:* {', '.join(conflict.get('agents_involved', []))}")
                    content.append("")
            
            if synthesis.get('data_gaps'):
                content.append("### Data Gaps Identified")
                content.append("")
                for gap in synthesis['data_gaps']:
                    content.append(f"- {gap}")
                content.append("")
            
            if synthesis.get('risk_assessments'):
                content.append("### Risk Assessments")
                content.append("")
                for risk in synthesis['risk_assessments']:
                    severity_emoji = "🔴" if risk.get('severity') == 'High' else "🟡" if risk.get('severity') == 'Medium' else "🟢"
                    content.append(f"{severity_emoji} **{risk.get('risk', 'N/A')}** [{risk.get('severity', 'Unknown')}]")
                    content.append(f"  - *Mitigation:* {risk.get('mitigation', 'N/A')}")
                    content.append("")
            
            if synthesis.get('key_insights_synthesis'):
                content.append("### Key Insights Synthesis")
                content.append("")
                for insight in synthesis['key_insights_synthesis']:
                    content.append(f"- **{insight.get('insight', 'N/A')}**")
                    if insight.get('supporting_agents'):
                        content.append(f"  - *Supported by:* {', '.join(insight['supporting_agents'])}")
                    if insight.get('evidence'):
                        content.append(f"  - *Evidence:* {insight['evidence']}")
                    content.append("")
            
            # Cross-Agent Correlations
            if synthesis.get('cross_agent_correlations'):
                content.append("### Cross-Agent Correlations & Causal Relationships")
                content.append("")
                for correlation in synthesis['cross_agent_correlations']:
                    corr_type = correlation.get('correlation_type', 'correlational')
                    type_emoji = "🔗" if corr_type == 'causal' else "📊" if corr_type == 'correlational' else "⏰"
                    content.append(f"{type_emoji} **{corr_type.upper()} Relationship**")
                    content.append("")
                    content.append(f"**Primary Insight:** {correlation.get('primary_insight', 'N/A')}")
                    content.append(f"**Correlated Insight:** {correlation.get('correlated_insight', 'N/A')}")
                    content.append(f"**Relationship:** {correlation.get('relationship', 'N/A')}")
                    if correlation.get('agents_involved'):
                        content.append(f"**Agents:** {', '.join(correlation['agents_involved'])}")
                    if correlation.get('confidence'):
                        content.append(f"**Confidence:** {correlation['confidence']}")
                    if correlation.get('evidence'):
                        content.append(f"**Evidence:** {correlation['evidence']}")
                    if correlation.get('implication'):
                        content.append(f"**Implication:** {correlation['implication']}")
                    content.append("")
            
            # Causal Chains
            if synthesis.get('causal_chains'):
                content.append("### Causal Chains")
                content.append("")
                for chain_obj in synthesis['causal_chains']:
                    content.append("**Causal Chain:**")
                    chain = chain_obj.get('chain', [])
                    for i, link in enumerate(chain):
                        arrow = "→" if i < len(chain) - 1 else ""
                        content.append(f"  {i+1}. [{link.get('agent', 'N/A')}] {link.get('finding', 'N/A')} {arrow}")
                    content.append("")
                    if chain_obj.get('root_cause'):
                        content.append(f"**Root Cause:** {chain_obj['root_cause']}")
                    if chain_obj.get('final_effect'):
                        content.append(f"**Final Effect:** {chain_obj['final_effect']}")
                    if chain_obj.get('recommended_action'):
                        content.append(f"**Recommended Action:** {chain_obj['recommended_action']}")
                    content.append("")
            
            content.append("---")
            content.append("")
        
        # Summary
        summary = results['summary']
        content.append("## Technical Summary")
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
                            
                            # Recommendations removed - only shown in Top 3 Prioritized Recommendations section
                            
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
        
        # Scores and Metrics Explanation
        content.append("## Scores and Metrics Explanation")
        content.append("")
        content.append("This section explains the different scores and metrics used throughout this report:")
        content.append("")
        
        content.append("### Engagement Score")
        content.append("")
        content.append("The engagement score is a composite metric (0-100 scale) that measures user engagement across multiple dimensions:")
        content.append("")
        content.append("**Formula:**")
        content.append("```")
        content.append("Engagement Score = (Session Frequency × 0.3) + (Session Duration × 0.3) + (Event Frequency × 0.2) + (Recency × 0.2)")
        content.append("```")
        content.append("")
        content.append("**Components:**")
        content.append("- **Session Frequency** (30% weight): Normalized measure of total session time")
        content.append("- **Session Duration** (30% weight): Average session duration in minutes")
        content.append("- **Event Frequency** (20% weight): Total number of events performed by user")
        content.append("- **Recency** (20% weight): How recently the user was active (more recent = higher score)")
        content.append("")
        content.append("**Interpretation:**")
        content.append("- **0-20**: Low engagement (churned or at-risk users)")
        content.append("- **20-40**: Moderate engagement (occasional users)")
        content.append("- **40-60**: Good engagement (regular users)")
        content.append("- **60-80**: High engagement (active users)")
        content.append("- **80-100**: Very high engagement (power users)")
        content.append("")
        
        content.append("### Recommendation Quality Scores")
        content.append("")
        content.append("All recommendations are validated using three quality scores (0-10 scale):")
        content.append("")
        content.append("**1. Specificity Score (40% weight)**")
        content.append("- Measures how specific and concrete the recommendation is")
        content.append("- **Increases** for: Specific dates, numbers, percentages, dollar amounts, metric names, cohort references")
        content.append("- **Decreases** for: Generic phrases like 'improve', 'enhance', 'optimize', 'implement strategies'")
        content.append("- **Base score:** 6.0 (adjusted based on specific indicators)")
        content.append("")
        content.append("**2. Actionability Score (40% weight)**")
        content.append("- Measures how easily the recommendation can be executed")
        content.append("- **Increases** for: Action verbs (implement, set up, trigger, deploy), specific timeframes, target metrics")
        content.append("- **Decreases** for: Vague phrases (focus on, consider, explore, investigate)")
        content.append("- **Base score:** 5.5 (adjusted based on actionability indicators)")
        content.append("")
        content.append("**3. Data Support Score (20% weight)**")
        content.append("- Measures how well the recommendation is supported by data evidence")
        content.append("- **Increases** for: Numbers/percentages in evidence, metric names, date/cohort references, comparisons (vs, compared to)")
        content.append("- **Base score:** 3.0 (adjusted based on evidence quality)")
        content.append("")
        content.append("**Total Score Calculation:**")
        content.append("```")
        content.append("Total Score = (Specificity × 0.4) + (Actionability × 0.4) + (Data Support × 0.2)")
        content.append("```")
        content.append("")
        content.append("**Filtering Thresholds (Strict Mode):**")
        content.append("- Recommendations must have **Total Score ≥ 6.0**")
        content.append("- Recommendations must have **Specificity ≥ 6.5** OR **Actionability ≥ 6.0**")
        content.append("- Generic recommendations with Total Score < 5.5 are filtered out")
        content.append("")
        
        content.append("### Other Metrics Used")
        content.append("")
        content.append("**ARPU (Average Revenue Per User):**")
        content.append("- Calculation: Total Revenue / Total Users")
        content.append("- Unit: Dollars per user")
        content.append("")
        content.append("**DAU (Daily Active Users):**")
        content.append("- Calculation: Count of unique users with activity on a given day")
        content.append("- Unit: Number of users")
        content.append("")
        content.append("**D1 Retention Rate:**")
        content.append("- Calculation: (Users who returned on Day 1 / Users who were new on Day 0) × 100")
        content.append("- Unit: Percentage")
        content.append("- Interpretation: Higher percentage indicates better user onboarding and engagement")
        content.append("")
        content.append("**Cohort Retention:**")
        content.append("- Calculation: Percentage of users from a specific cohort who remain active after N days")
        content.append("- Common metrics: D1, D7, D14, D30 retention")
        content.append("- Unit: Percentage")
        content.append("")
        
        content.append("---")
        content.append("")
        
        # Footer
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
