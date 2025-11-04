#!/usr/bin/env python3
"""
Daily Metrics Prompt Generator
Version: 2.1.0
Last Updated: 2025-10-31

Prompt generator for daily metrics analysis.
Sends entire filtered dataset to LLM (not just first 5 rows).
"""

import json
import pandas as pd
from typing import Dict, Any
from .base_generator import BasePromptGenerator

class DailyMetricsPromptGenerator(BasePromptGenerator):
    """Prompt generator for daily metrics analysis."""
    
    def __init__(self):
        super().__init__("daily_metrics")
    
    def format_data_for_prompt(self, data: Dict[str, Any], use_summaries: bool = True, max_rows: int = 100) -> str:
        """Format data for inclusion in prompts with token optimization."""
        return super().format_data_for_prompt(data, use_summaries=use_summaries, max_rows=max_rows)
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for daily metrics analysis with enhanced structure."""
        context = self.get_context_info(run_metadata)
        
        # Get few-shot examples (token-optimized: only if file exists)
        few_shot_examples = self.get_few_shot_examples()
        
        # Chain-of-thought instructions
        cot_instructions = """
ANALYSIS PROCESS (think step by step):

STEP 1: Data Inspection
- What are the actual numbers? List top 3 metrics and their values
- What are the trends? Increasing/decreasing/stable?
- Are there anomalies? List specific dates/values that stand out

STEP 2: Pattern Identification
- What patterns do you see? (Be specific: "Revenue drops 40% on weekends" not "revenue varies")
- Which segments/cohorts/regions perform differently? (List with numbers)

STEP 3: Root Cause Hypothesis
- Why might these patterns exist? (Based on data, not assumptions)
- What data supports your hypothesis? (Cite specific rows/values)

STEP 4: Recommendation Generation
- WHO: Which specific segment/cohort/date? Include percentage or size
- WHAT: What exact action? Use action verbs (implement, set up, test, launch)
- WHEN: What specific timing? Include dates or day numbers (e.g., "day 3 post-install", "starting 2025-11-10")
- EXPECTED OUTCOME: What metric changes? Include current and target values with numbers
- TIMEFRAME: How long? Use days/weeks, not vague terms
- EVIDENCE: Include at least 2 specific data points from the data provided

STEP 5: Validation
- Can this recommendation be executed next week? (If no, refine)
- Does it include specific numbers/dates/percentages? (If no, add them)
- Is it specific to THIS dataset? (If generic, discard)
- Does it reference actual data points? (If no, discard)
"""
        
        prompt_parts = [
            "# Daily Metrics Analysis",
            f"\n**Context:** {context}",
        ]
        
        # Add few-shot examples if available (token-optimized: concise)
        if few_shot_examples:
            prompt_parts.append(f"\n{few_shot_examples}")
        
        prompt_parts.extend([
            f"\n**Data Available:**",
            f"{self.format_data_for_prompt(data, use_summaries=True, max_rows=100)}",
            f"\n**Analysis Instructions:**",
            f"{self.get_analysis_instructions()}",
            f"\n{cot_instructions}",
            f"\n**Specific Focus Areas:**",
            "1. Daily trend analysis",
            "2. Metric correlation patterns",
            "3. Performance benchmarks",
            "4. Anomaly detection",
            "\nPlease provide a comprehensive analysis of the daily metrics data."
        ])
        
        return "\n".join(prompt_parts).strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for daily metrics analysis with enhanced constraints."""
        return """You are a product analytics consultant with 10+ years experience in mobile games and product analytics.

CRITICAL CONSTRAINTS:
- NEVER recommend: 'add features', 'improve UX', 'use ML/AI', 'implement strategies', 'deploy models' (too generic)
- ALWAYS specify: WHAT metric, WHEN to measure, EXPECTED change, TIME frame
- REQUIRED format: 'For [segment/date], [action] targeting [specific users] by [date] expecting [metric] change from [current] to [target] within [timeframe]'

GOOD RECOMMENDATION EXAMPLE:
'For high-engagement users (46.6% of base), implement a premium tier unlock prompt on day 3 post-install (when 67% reach level 5). Target ARPU increase from $0.15 to $0.35 within 30 days based on similar user conversion rates.'

BAD RECOMMENDATION (DO NOT GENERATE):
'Develop targeted upsell strategies for high engagement users' (too vague)

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
1. **Analyze Daily Trends**: Identify patterns, trends, and anomalies in daily metrics data
2. **Provide Insights**: Generate actionable insights based on the data
3. **Assess Performance**: Evaluate performance against benchmarks and historical data
4. **Identify Opportunities**: Highlight areas for improvement and optimization

**Output Format:**
Provide your analysis in the following JSON structure:

```json
{
  "analysis_type": "daily_metrics",
  "summary": "Brief overview of key findings",
  "trends": {
    "overall_trend": "description of overall trend",
    "key_metrics": ["list of key metrics with trends"],
    "anomalies": ["list of any anomalies detected"]
  },
  "insights": [
    {
      "metric": "metric name",
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
