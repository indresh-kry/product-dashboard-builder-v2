#!/usr/bin/env python3
"""
Child LLM 2: User Segmentation Analyst
Version: 1.0.0
Last Updated: 2025-10-16

Description:
Specialized LLM for analyzing user behavior segments and journey patterns.

Dependencies:
- openai: OpenAI API client
- pandas: Data manipulation
- json: JSON serialization
- os: Environment variable access
"""
import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ Warning: OpenAI library not available. Install with: pip install openai")

def call_openai_api(prompt: str) -> Dict:
    """Call OpenAI API to generate user segmentation insights."""
    print("🤖 Calling OpenAI API for user segmentation analysis...")
    
    client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a specialized user segmentation analyst with 10+ years of experience in behavioral analytics and user journey mapping. You excel at identifying user patterns, segment performance, and behavioral insights that drive product optimization. You ALWAYS respond with valid JSON format only, never with explanatory text or markdown."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        content = response.choices[0].message.content
        
        # Try to parse JSON response
        try:
            # First try direct JSON parsing
            return json.loads(content)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks
            import re
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(1))
                except json.JSONDecodeError:
                    pass
            
            # Try to find JSON object in the text
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(0))
                except json.JSONDecodeError:
                    pass
            
            # If all parsing fails, wrap in a basic structure
            return {
                "raw_response": content,
                "parsing_error": "Response was not in expected JSON format"
            }
            
    except Exception as e:
        raise Exception(f"OpenAI API call failed: {str(e)}")

def load_segmentation_data(run_hash: str) -> Dict:
    """Load user segmentation data from Phase 3 outputs."""
    print("👥 Loading user segmentation data...")
    
    data = {}
    
    # Load user journey cohort data
    journey_file = f'run_logs/{run_hash}/outputs/segments/user_level/user_journey_cohort.csv'
    if os.path.exists(journey_file):
        df_journey = pd.read_csv(journey_file)
        data['journey_data'] = df_journey.head(10).to_csv(index=False)
        data['journey_summary'] = {
            'total_users': len(df_journey),
            'unique_cohorts': df_journey['cohort_date'].nunique() if 'cohort_date' in df_journey.columns else 0
        }
    
    # Load revenue segments data
    revenue_file = f'run_logs/{run_hash}/outputs/segments/user_level/revenue_segments_daily.csv'
    if os.path.exists(revenue_file):
        df_revenue = pd.read_csv(revenue_file)
        data['revenue_segments_data'] = df_revenue.head(10).to_csv(index=False)
        if 'revenue_segment' in df_revenue.columns:
            data['segment_distribution'] = df_revenue['revenue_segment'].value_counts().to_dict()
    
    return data

def generate_segmentation_prompt(data: Dict, run_metadata: Dict) -> str:
    """Generate the user segmentation analysis prompt."""
    
    prompt = f"""# USER SEGMENTATION ANALYSIS

## CONTEXT
You are analyzing user behavior segments and journey patterns for a mobile app.

## RUN INFORMATION
- **Run Hash**: {run_metadata.get('run_hash', 'unknown')}
- **Analysis Period**: {run_metadata.get('date_range', 'unknown')}
- **Data Source**: {run_metadata.get('data_source', 'unknown')}

## USER JOURNEY DATA
```csv
{data.get('journey_data', 'No journey data available')}
```

## REVENUE SEGMENTS DATA
```csv
{data.get('revenue_segments_data', 'No revenue segments data available')}
```

## SEGMENT DISTRIBUTION
{json.dumps(data.get('segment_distribution', {}), indent=2)}

## SUMMARY METRICS
- **Total Users**: {data.get('journey_summary', {}).get('total_users', 'N/A')}
- **Unique Cohorts**: {data.get('journey_summary', {}).get('unique_cohorts', 'N/A')}

## ANALYSIS TASK
Analyze the user segmentation data and provide insights on:

1. **Segment Performance**: Compare performance across user segments
2. **User Journey Patterns**: Identify common paths and progression
3. **Behavioral Insights**: Key behavioral patterns and characteristics
4. **Segment Opportunities**: Opportunities for segment-specific strategies

## OUTPUT FORMAT
Provide a structured JSON response:

```json
{{
  "segment_analysis": {{
    "top_performing_segment": "segment_name",
    "segment_distribution": {{"segment1": 0.4, "segment2": 0.3}},
    "performance_gaps": ["gap1", "gap2"],
    "segment_health_score": 0.75
  }},
  "journey_insights": {{
    "common_paths": ["path1", "path2"],
    "drop_off_points": ["point1", "point2"],
    "success_factors": ["factor1", "factor2"],
    "journey_completion_rate": 0.65
  }},
  "behavioral_patterns": [
    {{
      "pattern": "Pattern description",
      "frequency": "High/Medium/Low",
      "impact": "High/Medium/Low"
    }}
  ],
  "recommendations": [
    {{
      "recommendation": "Specific recommendation",
      "target_segment": "segment_name",
      "impact": "High/Medium/Low",
      "effort": "High/Medium/Low"
    }}
  ],
  "confidence_score": 0.85
}}
```

## ANALYSIS GUIDELINES
- Focus on clear segment differences and patterns
- Identify actionable insights for segment-specific strategies
- Consider user journey progression and drop-off points
- Provide specific recommendations for each segment
- Highlight opportunities for personalization and targeting

Please analyze the user segmentation data and provide your insights."""
    
    return prompt

def analyze_user_segmentation(run_hash: str, run_metadata: Dict) -> Dict:
    """Analyze user segmentation using specialized LLM."""
    print("👥 Analyzing user segmentation...")
    
    # Load data
    data = load_segmentation_data(run_hash)
    
    # Generate prompt
    prompt = generate_segmentation_prompt(data, run_metadata)
    
    # Call LLM
    insights = call_openai_api(prompt)
    
    # Add metadata
    insights['metadata'] = {
        'generated_at': datetime.now().isoformat(),
        'run_hash': run_hash,
        'analyst_type': 'user_segmentation',
        'data_files_used': ['user_journey_cohort.csv', 'revenue_segments_daily.csv']
    }
    
    return insights

def main():
    """Main function for testing."""
    print("🚀 Starting User Segmentation Analyst v1.0.0")
    print("=" * 80)
    
    run_hash = os.environ.get('RUN_HASH', 'test')
    run_metadata = {
        'run_hash': run_hash,
        'date_range': '2025-09-15 to 2025-09-30',
        'data_source': 'phase_3_fallback'
    }
    
    try:
        insights = analyze_user_segmentation(run_hash, run_metadata)
        print("✅ User segmentation analysis completed!")
        print(f"📊 Insights: {json.dumps(insights, indent=2, default=str)}")
        return 0
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
