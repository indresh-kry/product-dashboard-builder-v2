#!/usr/bin/env python3
"""
Revenue Optimization Prompt Generator
Version: 2.1.0
Last Updated: 2025-10-31

Prompt generator for revenue optimization analysis.
Sends entire filtered dataset to LLM (not just first 5 rows).
"""

import json
import pandas as pd
from typing import Dict, Any
from .base_generator import BasePromptGenerator

class RevenueOptimizationPromptGenerator(BasePromptGenerator):
    """Prompt generator for revenue optimization analysis."""
    
    def __init__(self):
        super().__init__("revenue_optimization")
    
    def format_data_for_prompt(self, data: Dict[str, Any]) -> str:
        """Format data for inclusion in prompts - sends ENTIRE dataset for revenue optimization data."""
        if not data:
            return "No data available for analysis."
        
        formatted_sections = []
        
        for key, value in data.items():
            if key == 'summary':
                continue
                
            if isinstance(value, pd.DataFrame):
                # For revenue optimization data, send the ENTIRE dataset (not just head())
                if len(value) > 0:
                    formatted_sections.append(
                        f"**{key.replace('_', ' ').title()}:**\n"
                        f"Total rows: {len(value)}\n"
                        f"Columns: {', '.join(value.columns)}\n"
                        f"\n{value.to_string(index=False)}"
                    )
                else:
                    formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\nNo data available.")
            elif isinstance(value, dict):
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{json.dumps(value, indent=2)}")
            else:
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{str(value)}")
        
        return "\n\n".join(formatted_sections)
    
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

1. **Analyze Revenue Patterns**: Identify patterns and trends in revenue data as well as different revenue types
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
- DO NOT make recommendations for geographies with less than 100 daily users 
- AVOID generic recommendations like "add new features" or "improve existing ones"
- Focus on actionable insights with specific, measurable recommendations
- Keep language of output simple and avoid jargon
- Provide atleast 2 data points as evidence to support every finding
- Include concrete numbers, dates, and measurable outcomes
- Assess data quality with specific examples
- Include confidence levels in metadata
- Highlight any data limitations or concerns
- Make recommendations specific to the actual data patterns observed"""
