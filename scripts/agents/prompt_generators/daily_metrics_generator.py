#!/usr/bin/env python3
"""
Daily Metrics Prompt Generator
Version: 2.0.0
Last Updated: 2025-10-23

Prompt generator for daily metrics analysis.
Creates specialized prompts for daily metrics analysis tasks.
"""

import json
from typing import Dict, Any
from .base_generator import BasePromptGenerator

class DailyMetricsPromptGenerator(BasePromptGenerator):
    """Prompt generator for daily metrics analysis."""
    
    def __init__(self):
        super().__init__("daily_metrics")
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for daily metrics analysis."""
        context = self.get_context_info(run_metadata)
        
        prompt = f"""
# Daily Metrics Analysis

**Context:** {context}

**Data Available:**
{self.format_data_for_prompt(data)}

**Analysis Instructions:**
{self.get_analysis_instructions()}

**Specific Focus Areas:**
1. Daily trend analysis
2. Metric correlation patterns
3. Seasonal variations
4. Performance benchmarks
5. Anomaly detection

Please provide a comprehensive analysis of the daily metrics data.
"""
        return prompt.strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for daily metrics analysis."""
        return """You are a data analyst specializing in daily metrics analysis. Your role is to:

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
      "recommendation": "actionable recommendation"
    }
  ],
  "recommendations": [
    {
      "category": "category name",
      "priority": "High/Medium/Low",
      "action": "specific action to take",
      "expected_impact": "expected outcome"
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
- Focus on actionable insights
- Provide specific recommendations
- Assess data quality
- Include confidence levels in metadata
- Highlight any data limitations or concerns"""
