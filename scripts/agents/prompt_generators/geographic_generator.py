#!/usr/bin/env python3
"""
Geographic Prompt Generator
Version: 2.1.0
Last Updated: 2025-10-31

Prompt generator for geographic analysis.
Sends entire filtered dataset to LLM (not just first 5 rows).
"""

import json
import pandas as pd
from typing import Dict, Any, Optional
from .base_generator import BasePromptGenerator

class GeographicPromptGenerator(BasePromptGenerator):
    """Prompt generator for geographic analysis."""
    
    def __init__(self):
        super().__init__("geographic")
    
    def format_data_for_prompt(self, data: Dict[str, Any], use_summaries: bool = True, max_rows: int = 100) -> str:
        """Format data for inclusion in prompts with token optimization."""
        return super().format_data_for_prompt(data, use_summaries=use_summaries, max_rows=max_rows)
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any], charts_info: Optional[str] = None) -> str:
        """Generate prompt for geographic analysis with enhanced structure."""
        context = self.get_context_info(run_metadata)
        few_shot_examples = self.get_few_shot_examples()
        
        cot_instructions = """
ANALYSIS PROCESS (think step by step):

STEP 1: Data Inspection & Chart Review
- Review the provided charts to identify visual geographic patterns
- What are the actual numbers? List top 3 metrics and their values
- What are the trends visible in the country charts? (e.g., "The dau_by_country_top10.png chart shows...")
- Are there visual anomalies in the geographic charts? Reference specific chart names
- Compare country performance across different charts (e.g., dau_by_country_top10.png vs revenue_by_country_top10.png)

STEP 2: Pattern Identification Using Charts
- What patterns do you see in the geographic charts? (Be specific: "The revenue_by_country_arpu.png chart shows US has ARPU 3x higher than average" not "countries vary")
- Which countries/regions perform differently in the charts? (List with numbers and chart references)
- What does the new_logins_by_country_heatmap.png chart reveal about acquisition patterns?
- Compare revenue mix across countries using revenue_by_country_mix.png and revenue_by_country_iap_vs_ad.png

STEP 3: Root Cause Hypothesis with Chart Evidence
- Why might these geographic patterns exist? (Based on data AND chart observations)
- What chart visualizations support your hypothesis? (Cite specific charts and what they show)
- What data supports your hypothesis? (Cite specific rows/values)

STEP 4: Recommendation Generation
- WHO: Which specific country/region? Include percentage or size
- WHAT: What exact action? Use action verbs (implement, set up, test, launch)
- WHEN: What specific timing? Include dates or day numbers (e.g., "starting 2025-11-10")
- EXPECTED OUTCOME: What metric changes? Include current and target values with numbers
- TIMEFRAME: How long? Use days/weeks, not vague terms
- EVIDENCE: Include at least 2 specific data points AND reference relevant charts (e.g., "As shown in revenue_by_country_top10.png, US represents 45% of total revenue")

STEP 5: Validation
- Can this recommendation be executed next week? (If no, refine)
- Does it include specific numbers/dates/percentages? (If no, add them)
- Does it reference specific charts by name? (If no, add chart references)
- Is it specific to THIS dataset? (If generic, discard)
- Does it reference actual data points AND chart observations? (If no, discard)
"""
        
        prompt_parts = ["# Geographic Analysis", f"\n**Context:** {context}"]
        
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
            "1. Geographic distribution patterns (reference dau_by_country_top10.png and revenue_by_country_top10.png for country rankings)",
            "2. Regional performance differences (use revenue_by_country_arpu.png and dau_by_country_growth_trends.png to identify high-value regions)",
            "3. Location-based insights (analyze new_logins_by_country_heatmap.png to understand acquisition patterns)",
            "4. Geographic trends (review revenue_by_country_timeline_stacked.png and dau_by_country_timeline_stacked.png for country evolution)",
            "5. Market opportunities (compare revenue_by_country_mix.png and revenue_by_country_iap_vs_ad.png to identify revenue composition by country)",
            "\n**IMPORTANT:** When making observations, explicitly reference the chart name (e.g., 'As shown in revenue_by_country_top10.png...') and describe what you see visually. Use charts to support your recommendations with visual evidence."
        ])
        return "\n".join(prompt_parts).strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for geographic analysis with enhanced constraints."""
        return """You are a product analytics consultant with 10+ years experience in mobile games and product analytics.

CRITICAL CONSTRAINTS:
- NEVER recommend: 'add features', 'improve UX', 'use ML/AI', 'implement strategies', 'deploy models' (too generic)
- ALWAYS specify: WHAT metric, WHEN to measure, EXPECTED change, TIME frame
- REQUIRED format: 'For [segment/date], [action] targeting [specific users] by [date] expecting [metric] change from [current] to [target] within [timeframe]'
- DO NOT make recommendations for geographies with less than 100 daily users

GOOD RECOMMENDATION EXAMPLE:
'APAC region (23% of user base) shows ARPU $0.45 vs $1.12 global avg. Implement localized payment methods (Alipay, Paytm) for APAC users. Launch in India and Southeast Asia starting 2025-11-20, targeting ARPU increase from $0.45 to $0.78 within 60 days based on similar localization projects showing 73% ARPU increase.'

BAD RECOMMENDATION (DO NOT GENERATE):
'Focus on underperforming regions' (too vague)

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
1. **Analyze Geographic Patterns**: Identify patterns and trends in geographic data
2. **Provide Insights**: Generate actionable insights based on location data
3. **Assess Performance**: Evaluate performance across different regions
4. **Identify Opportunities**: Highlight geographic opportunities and challenges

**Output Format:**
Provide your analysis in the following JSON structure:

```json
{
  "analysis_type": "geographic",
  "summary": "Brief overview of key findings",
  "geographic_patterns": {
    "total_locations": "number of locations",
    "key_regions": ["list of key regions"],
    "distribution_patterns": "description of distribution patterns"
  },
  "insights": [
    {
      "region": "region name",
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
