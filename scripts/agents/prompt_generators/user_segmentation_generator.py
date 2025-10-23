#!/usr/bin/env python3
"""
User Segmentation Prompt Generator
Version: 2.0.0
Last Updated: 2025-10-23

Prompt generator for user segmentation analysis.
"""

from typing import Dict, Any
from .base_generator import BasePromptGenerator

class UserSegmentationPromptGenerator(BasePromptGenerator):
    """Prompt generator for user segmentation analysis."""
    
    def __init__(self):
        super().__init__("user_segmentation")
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate prompt for user segmentation analysis."""
        context = self.get_context_info(run_metadata)
        
        prompt = f"""
# User Segmentation Analysis

**Context:** {context}

**Data Available:**
{self.format_data_for_prompt(data)}

**Analysis Instructions:**
{self.get_analysis_instructions()}

**Specific Focus Areas:**
1. User behavior patterns
2. Segmentation effectiveness
3. User lifecycle analysis
4. Engagement metrics
5. Retention patterns

Please provide a comprehensive analysis of the user segmentation data.
"""
        return prompt.strip()
    
    def get_system_prompt(self) -> str:
        """Get system prompt for user segmentation analysis."""
        return """You are a data analyst specializing in user segmentation analysis. Your role is to:

1. **Analyze User Segments**: Identify patterns and behaviors within user segments
2. **Provide Insights**: Generate actionable insights based on segmentation data
3. **Assess Segmentation**: Evaluate the effectiveness of current segmentation
4. **Identify Opportunities**: Highlight areas for segmentation improvement

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
