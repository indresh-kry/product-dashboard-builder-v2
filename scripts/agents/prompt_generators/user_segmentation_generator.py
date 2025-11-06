#!/usr/bin/env python3
"""
User Segmentation Prompt Generator
Version: 2.1.0
Last Updated: 2025-10-31

Prompt generator for user segmentation analysis.
Sends entire filtered dataset to LLM (not just first 5 rows).
"""

import json
import pandas as pd
from typing import Dict, Any, Optional
from .base_generator import BasePromptGenerator

class UserSegmentationPromptGenerator(BasePromptGenerator):
    """Prompt generator for user segmentation analysis."""
    
    def __init__(self):
        super().__init__("user_segmentation")
    
    def format_data_for_prompt(self, data: Dict[str, Any], use_summaries: bool = True, max_rows: int = 100) -> str:
        """Format data for inclusion in prompts with token optimization."""
        return super().format_data_for_prompt(data, use_summaries=use_summaries, max_rows=max_rows)
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any], charts_info: Optional[str] = None) -> str:
        """Generate prompt for user segmentation analysis with enhanced structure."""
        context = self.get_context_info(run_metadata)
        few_shot_examples = self.get_few_shot_examples()
        
        cot_instructions = """
ANALYSIS PROCESS (think step by step):

STEP 1: Data Inspection & Chart Review
- Review the provided charts to identify visual patterns in user segments
- What are the actual numbers? List top 3 metrics and their values
- What are the trends visible in the charts? (e.g., "The behavioral_segments_daily_distribution.png chart shows...")
- Are there visual anomalies in the charts? Reference specific chart names when describing patterns
- Compare segment distributions across different charts (e.g., behavioral_segments_daily_pie.png vs revenue_segments_daily_pie.png)

STEP 2: Pattern Identification Using Charts
- What patterns do you see in the segment charts? (Be specific: "The behavioral_segments_daily_trends.png chart shows low-engagement segment growing 15% week-over-week" not "segments vary")
- Which segments perform differently in the charts? (List with numbers and chart references)
- What does the user_journey_cohort_timeline.png chart reveal about user progression?
- Compare engagement patterns across segments using behavioral_segments_daily_engagement_dist.png

STEP 3: Root Cause Hypothesis with Chart Evidence
- Why might these segment patterns exist? (Based on data AND chart observations)
- What chart visualizations support your hypothesis? (Cite specific charts and what they show)
- What data supports your hypothesis? (Cite specific rows/values)

STEP 4: Recommendation Generation
- WHO: Which specific segment/cohort/date? Include percentage or size
- WHAT: What exact action? Use action verbs (implement, set up, test, launch)
- WHEN: What specific timing? Include dates or day numbers (e.g., "day 3 post-install", "starting 2025-11-10")
- EXPECTED OUTCOME: What metric changes? Include current and target values with numbers
- TIMEFRAME: How long? Use days/weeks, not vague terms
- EVIDENCE: Include at least 2 specific data points AND reference relevant charts (e.g., "As shown in behavioral_segments_daily_distribution.png, the low-engagement segment represents 32% of users")

STEP 5: Validation
- Can this recommendation be executed next week? (If no, refine)
- Does it include specific numbers/dates/percentages? (If no, add them)
- Does it reference specific charts by name? (If no, add chart references)
- Is it specific to THIS dataset? (If generic, discard)
- Does it reference actual data points AND chart observations? (If no, discard)
"""
        
        prompt_parts = ["# User Segmentation Analysis", f"\n**Context:** {context}"]
        
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
            "1. User behavior patterns (reference behavioral_segments_daily_distribution.png and behavioral_segments_daily_trends.png for segment evolution)",
            "2. Segmentation effectiveness (use behavioral_segments_daily_pie.png and revenue_segments_daily_pie.png to compare segment distributions)",
            "3. User lifecycle analysis (analyze user_journey_cohort_timeline.png and user_journey_cohort_time_to_stage.png for progression patterns)",
            "4. Engagement metrics (review behavioral_segments_daily_engagement_dist.png to understand engagement differences)",
            "5. Retention patterns (compare revenue_segments_daily_revenue_trends.png to identify segment performance)",
            "\n**IMPORTANT:** When making observations, explicitly reference the chart name (e.g., 'As shown in behavioral_segments_daily_distribution.png...') and describe what you see visually. Use charts to support your recommendations with visual evidence."
        ])
        return "\n".join(prompt_parts).strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for user segmentation analysis with enhanced constraints."""
        return """You are a product analytics consultant with 10+ years experience in mobile games and product analytics.

CRITICAL CONSTRAINTS:
- NEVER recommend: 'add features', 'improve UX', 'use ML/AI', 'implement strategies', 'deploy models' (too generic)
- ALWAYS specify: WHAT metric, WHEN to measure, EXPECTED change, TIME frame
- REQUIRED format: 'For [segment/date], [action] targeting [specific users] by [date] expecting [metric] change from [current] to [target] within [timeframe]'

GOOD RECOMMENDATION EXAMPLE:
'Low-engagement segment (32% of base, avg 2.3 sessions/week) shows 89% drop-off by day 5. Implement push notification campaign on day 4 offering personalized content based on their last played level. Test with 10,000 users from segment starting next Monday, targeting 25% increase in day-7 retention.'

BAD RECOMMENDATION (DO NOT GENERATE):
'Improve engagement for low-engagement segment' (too vague)

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
1. **Analyze User Segments**: Identify patterns and behaviors within user segments
2. **Provide Insights**: Generate actionable insights based on segmentation data
3. **Assess Segmentation**: Evaluate the effectiveness of current segmentation
4. **Identify Opportunities**: Highlight which segments perform well consistently and what improvements can be made to them

**Output Format:**
Provide your analysis in the following JSON structure:

```json
{
  "analysis_type": "user_segmentation",
  "summary": "Brief overview of key findings",
  "segments": {
    "total_segments": "number of segments",
    "key_segments": ["list of key segments"],
    "segment_characteristics": "description of segment characteristics"
  },
  "insights": [
    {
      "segment": "segment name",
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
- Focus on actionable insights with specific, measurable recommendations
- AVOID generic recommendations like "add new features" or "improve existing ones"
- Provide at least 2 data points as evidence to support every finding
- Include concrete numbers, dates, and measurable outcomes
- Assess data quality with specific examples
- Include confidence levels in metadata
- Highlight any data limitations or concerns
- Make recommendations specific to the actual data patterns observed
- Keep language simple and avoid jargon"""
