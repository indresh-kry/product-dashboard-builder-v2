#!/usr/bin/env python3
"""
Data Quality Prompt Generator
Version: 2.0.0
Last Updated: 2025-10-23

Prompt generator for data quality analysis.
"""

from typing import Dict, Any
from .base_generator import BasePromptGenerator

class DataQualityPromptGenerator(BasePromptGenerator):
    """Prompt generator for data quality analysis."""
    
    def __init__(self):
        super().__init__("data_quality")
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for data quality analysis."""
        context = self.get_context_info(run_metadata)
        
        prompt = f"""
# Data Quality Analysis

**Context:** {context}

**Data Available:**
{self.format_data_for_prompt(data)}

**Analysis Instructions:**
{self.get_analysis_instructions()}

**Specific Focus Areas:**
1. Data completeness assessment
2. Data consistency evaluation
3. Data accuracy analysis
4. Data quality issues
5. Improvement recommendations

Please provide a comprehensive analysis of the data quality.
"""
        return prompt.strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for data quality analysis."""
        return """You are a data analyst specializing in data quality analysis. Your role is to:

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
- AVOID generic recommendations like "add new features" or "improve existing ones"
- Focus on actionable insights with specific, measurable recommendations
- Keep language of output simple and avoid jargon
- Provide atleast 2 data points as evidence to support every finding
- Include concrete numbers, dates, and measurable outcomes
- Assess data quality with specific examples
- Include confidence levels in metadata
- Highlight any data limitations or concerns
- Make recommendations specific to the actual data patterns observed
- Make recommendations while ignoring the first 7 days of data"""
