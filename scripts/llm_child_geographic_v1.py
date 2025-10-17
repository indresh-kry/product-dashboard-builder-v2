#!/usr/bin/env python3
"""
Child LLM 3: Geographic Analyst
Version: 1.0.0
Last Updated: 2025-10-16

Description:
Specialized LLM for analyzing performance by country and region.

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
    """Call OpenAI API to generate geographic insights."""
    print("🤖 Calling OpenAI API for geographic analysis...")
    
    client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a specialized geographic analyst with 10+ years of experience in international mobile app analytics and market expansion. You excel at identifying regional performance patterns, localization opportunities, and market expansion strategies. You ALWAYS respond with valid JSON format only, never with explanatory text or markdown."
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

def load_geographic_data(run_hash: str) -> Dict:
    """Load geographic data from Phase 3 outputs."""
    print("🌍 Loading geographic data...")
    
    data = {}
    
    # Load DAU by country
    dau_country_file = f'run_logs/{run_hash}/outputs/segments/daily/dau_by_country.csv'
    if os.path.exists(dau_country_file):
        df_dau_country = pd.read_csv(dau_country_file)
        data['dau_by_country_data'] = df_dau_country.head(10).to_csv(index=False)
        if 'country' in df_dau_country.columns:
            data['country_distribution'] = df_dau_country['country'].value_counts().to_dict()
    
    # Load revenue by country
    revenue_country_file = f'run_logs/{run_hash}/outputs/segments/daily/revenue_by_country.csv'
    if os.path.exists(revenue_country_file):
        df_revenue_country = pd.read_csv(revenue_country_file)
        data['revenue_by_country_data'] = df_revenue_country.head(10).to_csv(index=False)
    
    # Load new logins by country
    new_logins_file = f'run_logs/{run_hash}/outputs/segments/daily/new_logins_by_country.csv'
    if os.path.exists(new_logins_file):
        df_new_logins = pd.read_csv(new_logins_file)
        data['new_logins_by_country_data'] = df_new_logins.head(10).to_csv(index=False)
    
    return data

def generate_geographic_prompt(data: Dict, run_metadata: Dict) -> str:
    """Generate the geographic analysis prompt."""
    
    prompt = f"""# GEOGRAPHIC ANALYSIS

## CONTEXT
You are analyzing geographic performance patterns for a mobile app across different countries and regions.

## RUN INFORMATION
- **Run Hash**: {run_metadata.get('run_hash', 'unknown')}
- **Analysis Period**: {run_metadata.get('date_range', 'unknown')}
- **Data Source**: {run_metadata.get('data_source', 'unknown')}

## DAU BY COUNTRY DATA
```csv
{data.get('dau_by_country_data', 'No DAU by country data available')}
```

## REVENUE BY COUNTRY DATA
```csv
{data.get('revenue_by_country_data', 'No revenue by country data available')}
```

## NEW LOGINS BY COUNTRY DATA
```csv
{data.get('new_logins_by_country_data', 'No new logins by country data available')}
```

## COUNTRY DISTRIBUTION
{json.dumps(data.get('country_distribution', {}), indent=2)}

## ANALYSIS TASK
Analyze the geographic data and provide insights on:

1. **Market Concentration**: Primary and secondary markets
2. **Regional Performance**: Performance differences by region
3. **Localization Opportunities**: Areas needing localization
4. **Market Expansion**: Potential for new market entry

## OUTPUT FORMAT
Provide a structured JSON response:

```json
{{
  "market_analysis": {{
    "primary_markets": ["country1", "country2"],
    "secondary_markets": ["country3", "country4"],
    "market_concentration": 0.95,
    "emerging_markets": ["country5"],
    "market_diversity_score": 0.25
  }},
  "regional_insights": {{
    "performance_by_region": {{"region1": 0.8, "region2": 0.6}},
    "cultural_factors": ["factor1", "factor2"],
    "regional_trends": ["trend1", "trend2"]
  }},
  "localization_recommendations": [
    {{
      "country": "country_name",
      "priority": "High/Medium/Low",
      "recommendations": ["rec1", "rec2"],
      "expected_impact": "High/Medium/Low"
    }}
  ],
  "expansion_opportunities": [
    {{
      "market": "market_name",
      "opportunity_type": "user_growth/revenue_growth",
      "potential_size": "Large/Medium/Small",
      "entry_strategy": "strategy_description"
    }}
  ],
  "confidence_score": 0.85
}}
```

## ANALYSIS GUIDELINES
- Focus on clear geographic patterns and market concentration
- Identify specific localization needs and opportunities
- Consider cultural and regional factors
- Provide actionable market expansion strategies
- Highlight underserved or emerging markets

Please analyze the geographic data and provide your insights."""
    
    return prompt

def analyze_geographic(run_hash: str, run_metadata: Dict) -> Dict:
    """Analyze geographic performance using specialized LLM."""
    print("🌍 Analyzing geographic performance...")
    
    # Load data
    data = load_geographic_data(run_hash)
    
    # Generate prompt
    prompt = generate_geographic_prompt(data, run_metadata)
    
    # Call LLM
    insights = call_openai_api(prompt)
    
    # Add metadata
    insights['metadata'] = {
        'generated_at': datetime.now().isoformat(),
        'run_hash': run_hash,
        'analyst_type': 'geographic',
        'data_files_used': ['dau_by_country.csv', 'revenue_by_country.csv', 'new_logins_by_country.csv']
    }
    
    return insights

def main():
    """Main function for testing."""
    print("🚀 Starting Geographic Analyst v1.0.0")
    print("=" * 80)
    
    run_hash = os.environ.get('RUN_HASH', 'test')
    run_metadata = {
        'run_hash': run_hash,
        'date_range': '2025-09-15 to 2025-09-30',
        'data_source': 'phase_3_fallback'
    }
    
    try:
        insights = analyze_geographic(run_hash, run_metadata)
        print("✅ Geographic analysis completed!")
        print(f"📊 Insights: {json.dumps(insights, indent=2, default=str)}")
        return 0
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
