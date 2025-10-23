#!/usr/bin/env python3
"""
Geographic Prompt Generator
Version: 2.0.0
Last Updated: 2025-10-23

Prompt generator for geographic analysis.
"""

from typing import Dict, Any
from .base_generator import BasePromptGenerator

class GeographicPromptGenerator(BasePromptGenerator):
    """Prompt generator for geographic analysis."""
    
    def __init__(self):
        super().__init__("geographic")
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for geographic analysis."""
        context = self.get_context_info(run_metadata)
        
        prompt = f"""
# Geographic Analysis

**Context:** {context}

**Data Available:**
{self.format_data_for_prompt(data)}

**Analysis Instructions:**
{self.get_analysis_instructions()}

**Specific Focus Areas:**
1. Geographic distribution patterns
2. Regional performance differences
3. Location-based insights
4. Geographic trends
5. Market opportunities

Please provide a comprehensive analysis of the geographic data.
"""
        return prompt.strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for geographic analysis."""
        return """You are a data analyst specializing in geographic analysis. Your role is to:

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
