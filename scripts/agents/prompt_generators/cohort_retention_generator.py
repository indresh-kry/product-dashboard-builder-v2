#!/usr/bin/env python3
"""
Cohort Retention Prompt Generator
Version: 2.1.0
Last Updated: 2025-10-23

Prompt generator for cohort retention analysis.
Sends entire filtered dataset to LLM (not just first 5 rows).
"""

import json
import pandas as pd
from typing import Dict, Any, Optional
from .base_generator import BasePromptGenerator

class CohortRetentionPromptGenerator(BasePromptGenerator):
    """Prompt generator for cohort retention analysis."""
    
    def __init__(self):
        super().__init__("cohort_retention")
    
    def format_data_for_prompt(self, data: Dict[str, Any], use_summaries: bool = True, max_rows: int = 100) -> str:
        """Format data for inclusion in prompts with token optimization."""
        return super().format_data_for_prompt(data, use_summaries=use_summaries, max_rows=max_rows)
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any], charts_info: Optional[str] = None) -> str:
        """Generate prompt for cohort retention analysis with enhanced structure."""
        context = self.get_context_info(run_metadata)
        few_shot_examples = self.get_few_shot_examples()
        
        cot_instructions = """
ANALYSIS PROCESS (think step by step):

STEP 1: Data Inspection & Chart Review
- Review the provided charts to identify visual retention patterns
- What are the actual numbers? List top 3 metrics and their values
- What are the trends visible in the retention charts? (e.g., "The retention_by_cohort_date_curve.png chart shows...")
- Are there visual anomalies in the cohort charts? Reference specific chart names
- Compare retention patterns across different cohorts using retention_by_cohort_date_heatmap.png

STEP 2: Pattern Identification Using Charts
- What patterns do you see in the retention charts? (Be specific: "The retention_by_cohort_date_d1_trend.png chart shows D1 retention declining 15% over the last 4 cohorts" not "retention varies")
- Which cohorts perform differently in the charts? (List with numbers and chart references)
- What does the funnel_by_cohort_date_heatmap.png chart reveal about conversion patterns?
- Compare retention decay using dau_by_cohort_date_retention_curve.png and retention_by_cohort_date_comparison.png
- Analyze event progression using event_funnel_by_cohort_date_progression.png and event_funnel_by_cohort_date_dropoff.png

STEP 3: Root Cause Hypothesis with Chart Evidence
- Why might these retention patterns exist? (Based on data AND chart observations)
- What chart visualizations support your hypothesis? (Cite specific charts and what they show)
- What data supports your hypothesis? (Cite specific rows/values)

STEP 4: Recommendation Generation
- WHO: Which specific cohort/date? Include percentage or size
- WHAT: What exact action? Use action verbs (implement, set up, test, launch)
- WHEN: What specific timing? Include dates or day numbers (e.g., "day 3 post-install", "starting 2025-11-10")
- EXPECTED OUTCOME: What metric changes? Include current and target values with numbers
- TIMEFRAME: How long? Use days/weeks, not vague terms
- EVIDENCE: Include at least 2 specific data points AND reference relevant charts (e.g., "As shown in retention_by_cohort_date_curve.png, cohort 2025-08-15 has 43.57% D1 retention")

STEP 5: Validation
- Can this recommendation be executed next week? (If no, refine)
- Does it include specific numbers/dates/percentages? (If no, add them)
- Does it reference specific charts by name? (If no, add chart references)
- Is it specific to THIS dataset? (If generic, discard)
- Does it reference actual data points AND chart observations? (If no, discard)
"""
        
        prompt_parts = ["# Cohort Retention Analysis", f"\n**Context:** {context}"]
        
        # Add chart references if available
        if charts_info:
            prompt_parts.append(f"\n{charts_info}")
        
        if few_shot_examples:
            prompt_parts.append(f"\n{few_shot_examples}")
        prompt_parts.extend([
            f"\n**Data Available:**",
            f"{self.format_data_for_prompt(data, use_summaries=True, max_rows=100)}",
            f"\n**Analysis Instructions:**",
            f"{self.get_analysis_instructions()}",
            f"\n{cot_instructions}",
            f"\n**Specific Focus Areas (USE CHARTS TO SUPPORT YOUR ANALYSIS):**",
            "1. Cohort retention patterns (reference retention_by_cohort_date_curve.png and retention_by_cohort_date_heatmap.png for visual retention trends)",
            "2. Lifecycle analysis (use dau_by_cohort_date_retention_curve.png and engagement_by_cohort_date_score.png to understand cohort evolution)",
            "3. Retention trends (analyze retention_by_cohort_date_d1_trend.png and retention_by_cohort_date_d7_trend.png for retention rate changes)",
            "4. Cohort performance (review revenue_by_cohort_date_curve.png and engagement_by_cohort_date_sessions.png to identify high-performing cohorts)",
            "5. Funnel analysis (examine funnel_by_cohort_date_conversion.png and event_funnel_by_cohort_date_dropoff.png for conversion patterns)",
            "\n**IMPORTANT:** When making observations, explicitly reference the chart name (e.g., 'As shown in retention_by_cohort_date_curve.png...') and describe what you see visually. Use charts to support your recommendations with visual evidence."
        ])
        return "\n".join(prompt_parts).strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for cohort retention analysis with enhanced constraints."""
        return """You are a product analytics consultant with 10+ years experience in mobile games and product analytics.

CRITICAL CONSTRAINTS:
- NEVER recommend: 'add features', 'improve UX', 'use ML/AI', 'implement strategies', 'deploy models' (too generic)
- ALWAYS specify: WHAT metric, WHEN to measure, EXPECTED change, TIME frame
- REQUIRED format: 'For [segment/date], [action] targeting [specific users] by [date] expecting [metric] change from [current] to [target] within [timeframe]'

GOOD RECOMMENDATION EXAMPLE:
'Cohort 2025-08-15 shows 43.57% D1 retention vs 30% average. Replicate the onboarding flow A/B test variant that was active that day. Apply to new cohorts starting 2025-11-15, targeting D1 retention increase from 30% to 38% within 2 weeks.'

BAD RECOMMENDATION (DO NOT GENERATE):
'Increase retention for underperforming cohorts' (too vague)

OUTPUT REQUIREMENTS:
1. Each recommendation MUST include:
   - WHO: Specific user segment/cohort/date with percentage (e.g., "high-engagement users, 46.6% of base" or "cohort 2025-08-15")
   - WHAT: Concrete action (e.g., "implement push notification campaign" not "improve engagement")
   - WHEN: Specific timing (e.g., "on day 3 post-install" or "starting week of 2025-11-10")
   - EXPECTED OUTCOME: Metric change with numbers (e.g., "ARPU increase from $0.15 to $0.35")
   - TIMEFRAME: Specific duration (e.g., "within 30 days" not "next quarter")
   - EVIDENCE: At least 2 specific data points from the provided data
2. If data doesn't support a specific recommendation, state 'INSUFFICIENT DATA' rather than generic advice
3. ALWAYS include specific numbers, percentages, dates, or metrics in every recommendation

**Your Role:**
1. **Analyze Cohort Patterns**: Identify patterns and trends in cohort retention data
2. **Provide Insights**: Generate actionable insights based on cohort behavior
3. **Assess Retention**: Evaluate retention performance across cohorts
4. **Identify Opportunities**: Highlight opportunities for retention improvement

**Output Format:**
Provide your analysis in the following JSON structure:

```json
{
  "analysis_type": "cohort_retention",
  "summary": "Brief overview of key findings",
  "cohort_patterns": {
    "total_cohorts": "number of cohorts",
    "key_cohorts": ["list of key cohorts"],
    "retention_patterns": "description of retention patterns"
  },
  "insights": [
    {
      "cohort": "cohort name",
      "finding": "key finding",
      "impact": "High/Medium/Low",
      "recommendation": "actionable recommendation",
      "evidence": "specific data points, percentages, or metrics that support this finding"
    }
  ],
  "recommendations": [
    {
      "category": "category name",
      "priority": "High/Medium/Low",
      "action": "specific action to take",
      "expected_impact": "expected outcome",
      "evidence": "data-driven justification for this recommendation"
    }
  ],
  "data_quality": {
    "completeness": "assessment of data completeness",
    "consistency": "assessment of data consistency",
    "issues": ["list of any data quality issues"]
  },
  "metadata": "Additional context, confidence levels, or data quality notes"
}
```

**Key Requirements:**
- Focus on player retention data and trends specifically
- Focus on actionable insights with specific, measurable recommendations
- AVOID generic recommendations like "add new features" or "improve existing ones"
- Provide at least 2 data points as evidence to support every finding
- Include concrete numbers, dates, and measurable outcomes
- Assess data quality with specific examples
- Include confidence levels in metadata
- Highlight any data limitations or concerns
- Make recommendations specific to the actual data patterns observed
- Keep language simple and avoid jargon"""
