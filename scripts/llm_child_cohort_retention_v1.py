#!/usr/bin/env python3
"""
Child LLM 4: Cohort & Retention Analyst
Version: 1.0.0
Last Updated: 2025-10-16

Description:
Specialized LLM for analyzing user retention and cohort performance.

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
    """Call OpenAI API to generate cohort and retention insights."""
    print("🤖 Calling OpenAI API for cohort and retention analysis...")
    
    client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a specialized cohort and retention analyst with 10+ years of experience in user lifecycle analytics and retention optimization. You excel at analyzing retention curves, cohort performance, and churn prevention strategies. You ALWAYS respond with valid JSON format only, never with explanatory text or markdown."
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

def load_cohort_data(run_hash: str) -> Dict:
    """Load cohort and retention data from Phase 3 outputs."""
    print("📈 Loading cohort and retention data...")
    
    data = {}
    
    # Load DAU by cohort date
    dau_cohort_file = f'run_logs/{run_hash}/outputs/segments/cohort/dau_by_cohort_date.csv'
    if os.path.exists(dau_cohort_file):
        df_dau_cohort = pd.read_csv(dau_cohort_file)
        data['dau_by_cohort_data'] = df_dau_cohort.head(10).to_csv(index=False)
        data['cohort_summary'] = {
            'total_cohorts': len(df_dau_cohort),
            'avg_cohort_size': df_dau_cohort['cohort_size'].mean() if 'cohort_size' in df_dau_cohort.columns else 0
        }
    
    # Load engagement by cohort date
    engagement_cohort_file = f'run_logs/{run_hash}/outputs/segments/cohort/engagement_by_cohort_date.csv'
    if os.path.exists(engagement_cohort_file):
        df_engagement_cohort = pd.read_csv(engagement_cohort_file)
        data['engagement_by_cohort_data'] = df_engagement_cohort.head(10).to_csv(index=False)
    
    # Load revenue by cohort date
    revenue_cohort_file = f'run_logs/{run_hash}/outputs/segments/cohort/revenue_by_cohort_date.csv'
    if os.path.exists(revenue_cohort_file):
        df_revenue_cohort = pd.read_csv(revenue_cohort_file)
        data['revenue_by_cohort_data'] = df_revenue_cohort.head(10).to_csv(index=False)
    
    return data

def generate_cohort_prompt(data: Dict, run_metadata: Dict) -> str:
    """Generate the cohort and retention analysis prompt."""
    
    prompt = f"""# COHORT & RETENTION ANALYSIS

## CONTEXT
You are analyzing user retention and cohort performance for a mobile app.

## RUN INFORMATION
- **Run Hash**: {run_metadata.get('run_hash', 'unknown')}
- **Analysis Period**: {run_metadata.get('date_range', 'unknown')}
- **Data Source**: {run_metadata.get('data_source', 'unknown')}

## DAU BY COHORT DATA
```csv
{data.get('dau_by_cohort_data', 'No DAU by cohort data available')}
```

## ENGAGEMENT BY COHORT DATA
```csv
{data.get('engagement_by_cohort_data', 'No engagement by cohort data available')}
```

## REVENUE BY COHORT DATA
```csv
{data.get('revenue_by_cohort_data', 'No revenue by cohort data available')}
```

## COHORT SUMMARY
- **Total Cohorts**: {data.get('cohort_summary', {}).get('total_cohorts', 'N/A')}
- **Average Cohort Size**: {data.get('cohort_summary', {}).get('avg_cohort_size', 'N/A'):.0f}

## ANALYSIS TASK
Analyze the cohort and retention data and provide insights on:

1. **Retention Patterns**: Retention curve analysis and key milestones
2. **Cohort Performance**: Comparison of different cohort performance
3. **Churn Analysis**: Churn patterns and risk factors
4. **Retention Strategies**: Recommendations for improving retention

## OUTPUT FORMAT
Provide a structured JSON response:

```json
{{
  "retention_analysis": {{
    "retention_curve": {{"day_1": 0.8, "day_7": 0.6, "day_30": 0.4}},
    "key_retention_milestones": ["day_1", "day_7", "day_30"],
    "retention_health_score": 0.75,
    "retention_trend": "improving/stable/declining"
  }},
  "cohort_insights": {{
    "best_performing_cohorts": ["cohort1", "cohort2"],
    "cohort_trends": ["trend1", "trend2"],
    "cohort_size_impact": "positive/negative/neutral",
    "seasonal_patterns": ["pattern1", "pattern2"]
  }},
  "churn_analysis": {{
    "churn_patterns": ["pattern1", "pattern2"],
    "churn_risk_factors": ["factor1", "factor2"],
    "churn_prediction_accuracy": 0.8,
    "critical_churn_periods": ["period1", "period2"]
  }},
  "retention_strategies": [
    {{
      "strategy": "Strategy description",
      "target_period": "day_1/day_7/day_30",
      "expected_impact": "High/Medium/Low",
      "implementation_effort": "High/Medium/Low"
    }}
  ],
  "confidence_score": 0.85
}}
```

## ANALYSIS GUIDELINES
- Focus on clear retention patterns and cohort differences
- Identify specific churn risk factors and critical periods
- Provide actionable retention improvement strategies
- Consider cohort size and seasonal effects
- Highlight opportunities for retention optimization

Please analyze the cohort and retention data and provide your insights."""
    
    return prompt

def analyze_cohort_retention(run_hash: str, run_metadata: Dict) -> Dict:
    """Analyze cohort and retention using specialized LLM."""
    print("📈 Analyzing cohort and retention...")
    
    # Load data
    data = load_cohort_data(run_hash)
    
    # Generate prompt
    prompt = generate_cohort_prompt(data, run_metadata)
    
    # Call LLM
    insights = call_openai_api(prompt)
    
    # Add metadata
    insights['metadata'] = {
        'generated_at': datetime.now().isoformat(),
        'run_hash': run_hash,
        'analyst_type': 'cohort_retention',
        'data_files_used': ['dau_by_cohort_date.csv', 'engagement_by_cohort_date.csv', 'revenue_by_cohort_date.csv']
    }
    
    return insights

def main():
    """Main function for testing."""
    print("🚀 Starting Cohort & Retention Analyst v1.0.0")
    print("=" * 80)
    
    run_hash = os.environ.get('RUN_HASH', 'test')
    run_metadata = {
        'run_hash': run_hash,
        'date_range': '2025-09-15 to 2025-09-30',
        'data_source': 'phase_3_fallback'
    }
    
    try:
        insights = analyze_cohort_retention(run_hash, run_metadata)
        print("✅ Cohort and retention analysis completed!")
        print(f"📊 Insights: {json.dumps(insights, indent=2, default=str)}")
        return 0
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
