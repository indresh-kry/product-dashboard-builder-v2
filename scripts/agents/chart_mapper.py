#!/usr/bin/env python3
"""
Chart Mapper
Version: 1.0.0

Maps generated charts to specific agents for enriching insights.
"""

from typing import Dict, List, Optional, Any
from pathlib import Path

class ChartMapper:
    """Maps charts to agents based on agent type."""
    
    # Chart mapping configuration
    CHART_MAPPING = {
        "daily_metrics": {
            "primary": [
                "daily/dau_by_date_trend.png",
                "daily/dau_by_date_new_vs_returning.png",
                "daily/dau_by_date_growth_rate.png",
                "daily/dau_by_date_dau_vs_revenue.png",
                "daily/dau_by_date_weekly_pattern.png",
                "daily/engagement_by_date_score_trend.png",
                "daily/engagement_by_date_session_time.png",
                "daily/engagement_by_date_events_trend.png",
                "daily/engagement_by_date_engagement_vs_users.png",
                "daily/engagement_by_date_components.png",
                "daily/engagement_by_date_weekly_pattern.png"
            ],
            "secondary": [
                "daily/dau_by_country_top10.png",
                "daily/dau_by_country_growth_trends.png",
                "daily/new_logins_by_country_trend.png"
            ]
        },
        "revenue_optimization": {
            "primary": [
                "daily/revenue_by_date_trend.png",
                "daily/revenue_by_date_by_type_stacked.png",
                "daily/revenue_by_date_arpu_trend.png",
                "daily/revenue_by_date_revenue_vs_users.png",
                "daily/revenue_by_date_stream_comparison.png",
                "daily/revenue_by_country_top10.png",
                "daily/revenue_by_country_trend_lines.png",
                "daily/revenue_by_country_arpu.png",
                "daily/revenue_by_country_mix.png",
                "daily/revenue_by_country_revenue_vs_users.png",
                "daily/revenue_by_country_timeline_stacked.png",
                "daily/revenue_by_country_iap_vs_ad.png",
                "daily/revenue_by_type_segment_trend.png",
                "daily/revenue_by_type_segment_stacked.png",
                "daily/revenue_by_type_arpu_by_segment.png",
                "daily/revenue_by_type_users_vs_revenue.png",
                "daily/new_logins_by_country_arpu.png",
                "daily/new_logins_by_country_acquisition_vs_revenue.png"
            ],
            "secondary": [
                "cohort/revenue_by_cohort_date_curve.png",
                "cohort/revenue_by_cohort_date_total.png",
                "cohort/revenue_by_cohort_date_comparison.png",
                "cohort/revenue_by_cohort_country_top_countries.png"
            ]
        },
        "user_segmentation": {
            "primary": [
                "user_level/behavioral_segments_daily_distribution.png",
                "user_level/behavioral_segments_daily_pie.png",
                "user_level/behavioral_segments_daily_engagement_dist.png",
                "user_level/behavioral_segments_daily_trends.png",
                "user_level/revenue_segments_daily_distribution.png",
                "user_level/revenue_segments_daily_pie.png",
                "user_level/revenue_segments_daily_revenue_trends.png",
                "user_level/revenue_segments_daily_revenue_mix.png",
                "user_level/user_journey_cohort_timeline.png",
                "user_level/user_journey_cohort_time_to_stage.png",
                "user_level/user_journey_cohort_progression_rate.png"
            ],
            "secondary": [
                "daily/engagement_by_date_score_trend.png",
                "daily/engagement_by_date_distribution.png",
                "cohort/engagement_by_cohort_date_score.png",
                "cohort/engagement_by_cohort_date_sessions.png"
            ]
        },
        "geographic": {
            "primary": [
                "daily/dau_by_country_top10.png",
                "daily/dau_by_country_growth_trends.png",
                "daily/dau_by_country_new_user_pct.png",
                "daily/dau_by_country_revenue_vs_dau.png",
                "daily/dau_by_country_timeline_stacked.png",
                "daily/revenue_by_country_top10.png",
                "daily/revenue_by_country_trend_lines.png",
                "daily/revenue_by_country_arpu.png",
                "daily/revenue_by_country_mix.png",
                "daily/revenue_by_country_timeline_stacked.png",
                "daily/revenue_by_country_iap_vs_ad.png",
                "daily/new_logins_by_country_trend.png",
                "daily/new_logins_by_country_heatmap.png",
                "daily/new_logins_by_country_arpu.png",
                "daily/new_logins_by_country_timeline_stacked.png",
                "cohort/revenue_by_cohort_country_top_countries.png",
                "cohort/revenue_by_cohort_country_trends.png",
                "cohort/revenue_by_cohort_country_contribution.png"
            ],
            "secondary": [
                "daily/engagement_by_date_weekly_pattern.png"
            ]
        },
        "cohort_retention": {
            "primary": [
                "cohort/dau_by_cohort_date_retention_curve.png",
                "cohort/dau_by_cohort_date_size_trend.png",
                "cohort/dau_by_cohort_date_comparison.png",
                "cohort/dau_by_cohort_date_size_vs_dau.png",
                "cohort/dau_by_cohort_date_top_cohorts.png",
                "cohort/retention_by_cohort_date_curve.png",
                "cohort/retention_by_cohort_date_heatmap.png",
                "cohort/retention_by_cohort_date_comparison.png",
                "cohort/retention_by_cohort_date_d1_trend.png",
                "cohort/retention_by_cohort_date_d7_trend.png",
                "cohort/retention_by_cohort_date_size_vs_retention.png",
                "cohort/revenue_by_cohort_date_curve.png",
                "cohort/revenue_by_cohort_date_comparison.png",
                "cohort/revenue_by_cohort_date_size_vs_revenue.png",
                "cohort/engagement_by_cohort_date_score.png",
                "cohort/engagement_by_cohort_date_sessions.png",
                "cohort/engagement_by_cohort_date_correlation.png",
                "cohort/funnel_by_cohort_date_conversion.png",
                "cohort/funnel_by_cohort_date_heatmap.png",
                "cohort/event_funnel_by_cohort_date_progression.png",
                "cohort/event_funnel_by_cohort_date_dropoff.png"
            ],
            "secondary": [
                "user_level/user_journey_cohort_timeline.png",
                "user_level/user_journey_cohort_time_to_stage.png"
            ]
        },
        "data_quality": {
            "primary": [],  # Data quality agent uses all charts for context
            "secondary": [
                "daily/dau_by_date_trend.png",
                "daily/revenue_by_date_trend.png",
                "cohort/retention_by_cohort_date_heatmap.png"
            ]
        }
    }
    
    def __init__(self, run_hash: str):
        self.run_hash = run_hash
        # Determine chart base directory
        script_dir = Path(__file__).parent.parent
        if (script_dir / "run_logs").exists():
            self.charts_base_dir = script_dir / "run_logs" / run_hash / "outputs" / "insights" / "segment_charts"
        else:
            self.charts_base_dir = Path(f"run_logs/{run_hash}/outputs/insights/segment_charts")
    
    def get_charts_for_agent(self, agent_type: str) -> Dict[str, List[str]]:
        """
        Get chart paths for a specific agent.
        Returns dict with 'primary' and 'secondary' chart lists.
        """
        mapping = self.CHART_MAPPING.get(agent_type, {"primary": [], "secondary": []})
        
        # Filter to only existing charts
        result = {"primary": [], "secondary": []}
        
        for chart_type in ["primary", "secondary"]:
            for chart_path in mapping.get(chart_type, []):
                full_path = self.charts_base_dir / chart_path
                if full_path.exists():
                    result[chart_type].append(str(chart_path))
        
        return result
    
    def format_charts_for_prompt(self, agent_type: str) -> str:
        """
        Format chart references for inclusion in LLM prompts.
        Returns a formatted string describing available charts.
        """
        charts = self.get_charts_for_agent(agent_type)
        
        if not charts["primary"] and not charts["secondary"]:
            return ""
        
        sections = []
        
        if charts["primary"]:
            sections.append("## PRIMARY VISUALIZATIONS")
            sections.append("The following charts are directly relevant to your analysis:")
            for i, chart in enumerate(charts["primary"], 1):
                chart_name = Path(chart).stem.replace("_", " ").title()
                sections.append(f"{i}. {chart_name} - Available at: {chart}")
        
        if charts["secondary"]:
            sections.append("\n## SECONDARY VISUALIZATIONS")
            sections.append("The following charts provide additional context:")
            for i, chart in enumerate(charts["secondary"], 1):
                chart_name = Path(chart).stem.replace("_", " ").title()
                sections.append(f"{i}. {chart_name} - Available at: {chart}")
        
        sections.append("\nIMPORTANT: When referencing these visualizations in your insights:")
        sections.append("- ALWAYS mention specific chart names when making observations (e.g., 'As shown in [chart_name.png]...')")
        sections.append("- Describe what you see visually in the charts (trends, spikes, drops, patterns, correlations)")
        sections.append("- Use chart observations to support your recommendations with visual evidence")
        sections.append("- Note any trends, anomalies, or correlations visible in the charts")
        sections.append("- Quantify observations when possible (e.g., 'the chart shows a 15% decline')")
        sections.append("- Compare patterns across multiple charts when relevant")
        sections.append("- When making recommendations, cite which chart(s) support your hypothesis")
        
        return "\n".join(sections)
    
    def get_chart_summary(self, agent_type: str) -> Dict[str, Any]:
        """Get summary of charts available for an agent."""
        charts = self.get_charts_for_agent(agent_type)
        return {
            "agent_type": agent_type,
            "primary_charts_count": len(charts["primary"]),
            "secondary_charts_count": len(charts["secondary"]),
            "total_charts": len(charts["primary"]) + len(charts["secondary"]),
            "primary_charts": charts["primary"],
            "secondary_charts": charts["secondary"]
        }

