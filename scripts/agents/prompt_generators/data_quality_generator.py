#!/usr/bin/env python3
"""
Data Quality Prompt Generator
Version: 2.1.0
Last Updated: 2025-10-31

Prompt generator for data quality analysis.
Sends entire filtered dataset to LLM (not just first 5 rows).
"""

import json
import pandas as pd
from typing import Dict, Any
from .base_generator import BasePromptGenerator

class DataQualityPromptGenerator(BasePromptGenerator):
    """Prompt generator for data quality analysis."""
    
    def __init__(self):
        super().__init__("data_quality")
    
    def format_data_for_prompt(self, data: Dict[str, Any], use_summaries: bool = True, max_rows: int = 100) -> str:
        """Format data for inclusion in prompts with token optimization."""
        return super().format_data_for_prompt(data, use_summaries=use_summaries, max_rows=max_rows)
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for data quality analysis with enhanced structure."""
        context = self.get_context_info(run_metadata)
        few_shot_examples = self.get_few_shot_examples()
        
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
        
        prompt_parts = ["# Data Quality Analysis", f"\n**Context:** {context}"]
        if few_shot_examples:
            prompt_parts.append(f"\n{few_shot_examples}")
        prompt_parts.extend([
            f"\n**Data Available:**",
            f"{self.format_data_for_prompt(data, use_summaries=True, max_rows=100)}",
            f"\n**Analysis Instructions:**",
            f"{self.get_analysis_instructions()}",
            f"\n{cot_instructions}",
            f"\n**Specific Focus Areas:**",
            "1. Data completeness assessment",
            "2. Data consistency evaluation",
            "3. Data accuracy analysis",
            "4. Data quality issues",
            "5. Improvement recommendations",
            "\nPlease provide a comprehensive analysis of the data quality."
        ])
        return "\n".join(prompt_parts).strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for data quality analysis with enhanced constraints."""
        return """You are a product analytics consultant with 10+ years experience in mobile games and product analytics.

CRITICAL CONSTRAINTS:
- NEVER recommend: 'add features', 'improve UX', 'use ML/AI', 'implement strategies', 'deploy models' (too generic)
- ALWAYS specify: WHAT metric, WHEN to measure, EXPECTED change, TIME frame
- REQUIRED format: 'For [segment/date], [action] targeting [specific users] by [date] expecting [metric] change from [current] to [target] within [timeframe]'

GOOD RECOMMENDATION EXAMPLE:
'Revenue data shows 12.3% missing values in revenue_amount column for dates 2025-09-10 to 2025-09-15. Implement data validation check in ETL pipeline to flag missing revenue_amount when purchase event exists. Fix historical gaps by backfilling from transaction logs. Complete within 7 days to prevent revenue reporting inaccuracies.'

BAD RECOMMENDATION (DO NOT GENERATE):
'Improve data quality' (too vague)

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
1. **Analyze Data Quality**: Identify data quality issues and patterns
2. **Provide Insights**: Generate actionable insights for data improvement
3. **Assess Completeness**: Evaluate data completeness and consistency
4. **Identify Issues**: Highlight specific data quality problems

**Output Format:**
Provide your analysis in the following JSON structure:

```json
{
  "analysis_type": "data_quality",
  "summary": "Brief overview of key findings",
  "quality_assessment": {
    "overall_score": "overall quality score",
    "key_issues": ["list of key quality issues"],
    "completeness": "assessment of data completeness"
  },
  "insights": [
    {
      "issue": "issue name",
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
