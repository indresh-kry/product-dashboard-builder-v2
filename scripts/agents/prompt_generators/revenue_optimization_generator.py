#!/usr/bin/env python3
"""
Revenue Optimization Prompt Generator
Version: 2.0.0
Last Updated: 2025-10-23

Prompt generator for revenue optimization analysis.
"""

from typing import Dict, Any
from .base_generator import BasePromptGenerator

class RevenueOptimizationPromptGenerator(BasePromptGenerator):
    """Prompt generator for revenue optimization analysis."""
    
    def __init__(self):
        super().__init__("revenue_optimization")
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for revenue optimization analysis."""
        context = self.get_context_info(run_metadata)
        
        prompt = f"""
# Revenue Optimization Analysis

**Context:** {context}

**Data Available:**
{self.format_data_for_prompt(data)}

**Analysis Instructions:**
{self.get_analysis_instructions()}

**Specific Focus Areas:**
1. Revenue trends and patterns
2. Optimization opportunities
3. Revenue drivers
4. Performance metrics
5. Growth potential

Please provide a comprehensive analysis of the revenue optimization data.
"""
        return prompt.strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for revenue optimization analysis."""
        return """You are a data analyst specializing in revenue optimization analysis. Your role is to:

1. **Analyze Revenue Patterns**: Identify patterns and trends in revenue data
2. **Provide Insights**: Generate actionable insights for revenue optimization
3. **Assess Performance**: Evaluate revenue performance and opportunities
4. **Identify Opportunities**: Highlight areas for revenue improvement

**Output Format:**
Provide your analysis in the following JSON structure:

```json
{
  "analysis_type": "revenue_optimization",
  "summary": "Brief overview of key findings",
  "revenue_patterns": {
    "total_revenue": "total revenue amount",
    "key_metrics": ["list of key revenue metrics"],
    "trends": "description of revenue trends"
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
- Provide specific evidence (data points, percentages, metrics) to support every finding
- Include concrete numbers, dates, and measurable outcomes
- Assess data quality with specific examples
- Include confidence levels in metadata
- Highlight any data limitations or concerns
- Make recommendations specific to the actual data patterns observed"""
