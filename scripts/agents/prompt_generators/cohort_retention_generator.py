#!/usr/bin/env python3
"""
Cohort Retention Prompt Generator
Version: 2.0.0
Last Updated: 2025-10-23

Prompt generator for cohort retention analysis.
"""

from typing import Dict, Any
from .base_generator import BasePromptGenerator

class CohortRetentionPromptGenerator(BasePromptGenerator):
    """Prompt generator for cohort retention analysis."""
    
    def __init__(self):
        super().__init__("cohort_retention")
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for cohort retention analysis."""
        context = self.get_context_info(run_metadata)
        
        prompt = f"""
# Cohort Retention Analysis

**Context:** {context}

**Data Available:**
{self.format_data_for_prompt(data)}

**Analysis Instructions:**
{self.get_analysis_instructions()}

**Specific Focus Areas:**
1. Cohort retention patterns
2. Lifecycle analysis
3. Retention trends
4. Cohort performance
5. Engagement metrics

Please provide a comprehensive analysis of the cohort retention data.
"""
        return prompt.strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for cohort retention analysis."""
        return """You are a data analyst specializing in cohort retention analysis. Your role is to:

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
- Focus on actionable insights with specific, measurable recommendations
- AVOID generic recommendations like "add new features" or "improve existing ones"
- Provide specific evidence (data points, percentages, metrics) to support every finding
- Include concrete numbers, dates, and measurable outcomes
- Assess data quality with specific examples
- Include confidence levels in metadata
- Highlight any data limitations or concerns
- Make recommendations specific to the actual data patterns observed"""
