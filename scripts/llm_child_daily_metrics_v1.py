#!/usr/bin/env python3
"""
Child LLM 1: Daily Metrics Analyst
Version: 1.0.0
Last Updated: 2025-10-16

Description:
Specialized LLM for analyzing daily active users, engagement trends, and core KPIs.

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
    """Call OpenAI API to generate daily metrics insights."""
    print("🤖 Calling OpenAI API for daily metrics analysis...")
    
    client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a specialized daily metrics analyst with 10+ years of experience in mobile app analytics. You excel at analyzing DAU trends, engagement patterns, and core KPIs. You provide clear, data-driven insights about user activity patterns and growth trends. You ALWAYS respond with valid JSON format only, never with explanatory text or markdown."
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

def load_daily_metrics_data(run_hash: str) -> Dict:
    """Load daily metrics data from Phase 3 outputs."""
    print("📊 Loading daily metrics data...")
    
    data = {}
    
    # Load DAU by date
    dau_file = f'run_logs/{run_hash}/outputs/segments/daily/dau_by_date.csv'
    if os.path.exists(dau_file):
        df_dau = pd.read_csv(dau_file)
        data['dau_data'] = df_dau.head(10).to_csv(index=False)
        data['dau_summary'] = {
            'total_dau': df_dau['total_dau'].sum() if 'total_dau' in df_dau.columns else 0,
            'avg_dau': df_dau['total_dau'].mean() if 'total_dau' in df_dau.columns else 0,
            'max_dau': df_dau['total_dau'].max() if 'total_dau' in df_dau.columns else 0,
            'min_dau': df_dau['total_dau'].min() if 'total_dau' in df_dau.columns else 0
        }
    
    # Load engagement by date
    engagement_file = f'run_logs/{run_hash}/outputs/segments/daily/engagement_by_date.csv'
    if os.path.exists(engagement_file):
        df_engagement = pd.read_csv(engagement_file)
        data['engagement_data'] = df_engagement.head(10).to_csv(index=False)
    
    return data

def generate_daily_metrics_prompt(data: Dict, run_metadata: Dict) -> str:
    """Generate the daily metrics analysis prompt."""
    
    prompt = f"""# DAILY METRICS ANALYSIS

## CONTEXT
You are analyzing daily active user trends and engagement patterns for a mobile app.

## RUN INFORMATION
- **Run Hash**: {run_metadata.get('run_hash', 'unknown')}
- **Analysis Period**: {run_metadata.get('date_range', 'unknown')}
- **Data Source**: {run_metadata.get('data_source', 'unknown')}

## DAILY ACTIVE USERS DATA
```csv
{data.get('dau_data', 'No DAU data available')}
```

## ENGAGEMENT DATA
```csv
{data.get('engagement_data', 'No engagement data available')}
```

## SUMMARY METRICS
- **Total DAU**: {data.get('dau_summary', {}).get('total_dau', 'N/A')}
- **Average DAU**: {data.get('dau_summary', {}).get('avg_dau', 'N/A'):.0f}
- **Peak DAU**: {data.get('dau_summary', {}).get('max_dau', 'N/A')}
- **Minimum DAU**: {data.get('dau_summary', {}).get('min_dau', 'N/A')}

## ANALYSIS TASK
Analyze the daily metrics data and provide insights on:

1. **DAU Trends**: Growth/decline patterns, consistency, seasonality
2. **Engagement Patterns**: User activity consistency, peak periods
3. **Growth Indicators**: New vs returning user trends
4. **Performance Metrics**: Key KPIs and their trends

## OUTPUT FORMAT
Provide a structured JSON response:

```json
{{
  "trend_analysis": {{
    "dau_trend": "growing/stable/declining",
    "growth_rate": "X%",
    "consistency_score": 0.85,
    "seasonality_detected": true/false,
    "peak_periods": ["day1", "day2"]
  }},
  "key_metrics": {{
    "avg_dau": 650,
    "new_user_ratio": 0.45,
    "engagement_stability": 0.78,
    "growth_velocity": "positive/negative/neutral"
  }},
  "insights": [
    {{
      "insight": "Specific insight about DAU patterns",
      "data_point": "Supporting data",
      "significance": "High/Medium/Low"
    }}
  ],
  "recommendations": [
    {{
      "recommendation": "Specific recommendation",
      "impact": "High/Medium/Low",
      "effort": "High/Medium/Low"
    }}
  ],
  "confidence_score": 0.85
}}
```

## ANALYSIS GUIDELINES
- Focus on clear trends and patterns in the data
- Provide specific numbers and percentages
- Identify actionable insights for product improvement
- Consider both short-term fluctuations and long-term trends
- Highlight any concerning patterns or opportunities

Please analyze the daily metrics data and provide your insights."""
    
    return prompt

def analyze_daily_metrics(run_hash: str, run_metadata: Dict) -> Dict:
    """Analyze daily metrics using specialized LLM."""
    print("📈 Analyzing daily metrics...")
    
    # Load data
    data = load_daily_metrics_data(run_hash)
    
    # Generate prompt
    prompt = generate_daily_metrics_prompt(data, run_metadata)
    
    # Call LLM
    insights = call_openai_api(prompt)
    
    # Add metadata
    insights['metadata'] = {
        'generated_at': datetime.now().isoformat(),
        'run_hash': run_hash,
        'analyst_type': 'daily_metrics',
        'data_files_used': ['dau_by_date.csv', 'engagement_by_date.csv']
    }
    
    return insights

def main():
    """Main function for testing."""
    print("🚀 Starting Daily Metrics Analyst v1.0.0")
    print("=" * 80)
    
    run_hash = os.environ.get('RUN_HASH', 'test')
    run_metadata = {
        'run_hash': run_hash,
        'date_range': '2025-09-15 to 2025-09-30',
        'data_source': 'phase_3_fallback'
    }
    
    try:
        insights = analyze_daily_metrics(run_hash, run_metadata)
        print("✅ Daily metrics analysis completed!")
        print(f"📊 Insights: {json.dumps(insights, indent=2, default=str)}")
        return 0
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
