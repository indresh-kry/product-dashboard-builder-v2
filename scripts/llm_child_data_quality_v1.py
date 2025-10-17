#!/usr/bin/env python3
"""
Child LLM 6: Data Quality & Improvement Analyst
Version: 1.0.0
Last Updated: 2025-10-16

Description:
Specialized LLM for analyzing data quality and suggesting improvements.

Dependencies:
- openai: OpenAI API client
- json: JSON serialization
- os: Environment variable access
"""
import os
import json
from datetime import datetime
from typing import Dict, List

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ Warning: OpenAI library not available. Install with: pip install openai")

def call_openai_api(prompt: str) -> Dict:
    """Call OpenAI API to generate data quality insights."""
    print("🤖 Calling OpenAI API for data quality analysis...")
    
    client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a specialized data quality analyst with 10+ years of experience in data governance, quality assessment, and analytics infrastructure. You excel at identifying data quality issues, sample size adequacy, and recommending improvements to data collection and analytics systems. You ALWAYS respond with valid JSON format only, never with explanatory text or markdown."
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

def load_data_quality_metrics(run_hash: str) -> Dict:
    """Load data quality metrics from schema discovery and other sources."""
    print("🔍 Loading data quality metrics...")
    
    data = {}
    
    # Load schema mapping for data quality info
    schema_file = f'run_logs/{run_hash}/outputs/schema/schema_mapping.json'
    if os.path.exists(schema_file):
        with open(schema_file, 'r') as f:
            schema_mapping = json.load(f)
            data['schema_quality'] = schema_mapping.get('data_quality', {})
            data['recommendations'] = schema_mapping.get('recommendations', {})
    
    # Load aggregation summary for sample size info
    aggregation_file = f'run_logs/{run_hash}/outputs/aggregations/aggregation_summary.json'
    if os.path.exists(aggregation_file):
        with open(aggregation_file, 'r') as f:
            aggregation_summary = json.load(f)
            data['aggregation_quality'] = aggregation_summary.get('data_quality_summary', {})
    
    # Load segmentation summary for additional quality metrics
    segmentation_file = f'run_logs/{run_hash}/outputs/segments/segmentation_summary.json'
    if os.path.exists(segmentation_file):
        with open(segmentation_file, 'r') as f:
            segmentation_summary = json.load(f)
            data['segmentation_quality'] = segmentation_summary.get('data_quality', {})
    
    return data

def generate_data_quality_prompt(data: Dict, run_metadata: Dict) -> str:
    """Generate the data quality analysis prompt."""
    
    prompt = f"""# DATA QUALITY & IMPROVEMENT ANALYSIS

## CONTEXT
You are analyzing data quality and suggesting improvements for a mobile app analytics system.

## RUN INFORMATION
- **Run Hash**: {run_metadata.get('run_hash', 'unknown')}
- **Analysis Period**: {run_metadata.get('date_range', 'unknown')}
- **Data Source**: {run_metadata.get('data_source', 'unknown')}

## DATA QUALITY METRICS

### Schema Quality
{json.dumps(data.get('schema_quality', {}), indent=2)}

### Aggregation Quality
{json.dumps(data.get('aggregation_quality', {}), indent=2)}

### Segmentation Quality
{json.dumps(data.get('segmentation_quality', {}), indent=2)}

### Recommendations from Schema Discovery
{json.dumps(data.get('recommendations', {}), indent=2)}

## ANALYSIS TASK
Analyze the data quality metrics and provide insights on:

1. **Data Quality Assessment**: Overall quality score and key issues
2. **Sample Size Adequacy**: Whether sample sizes are sufficient for analysis
3. **Data Collection Gaps**: Missing or incomplete data areas
4. **Analytics Infrastructure**: Recommendations for system improvements

## OUTPUT FORMAT
Provide a structured JSON response:

```json
{{
  "quality_assessment": {{
    "overall_quality_score": 0.85,
    "quality_issues": ["issue1", "issue2"],
    "sample_adequacy": {{"sufficient": true, "min_sample_size": 1000}},
    "data_completeness": 0.9,
    "quality_trend": "improving/stable/declining"
  }},
  "improvement_recommendations": {{
    "data_collection": [
      {{
        "recommendation": "Data collection improvement",
        "priority": "High/Medium/Low",
        "impact": "High/Medium/Low"
      }}
    ],
    "annotation_improvements": [
      {{
        "recommendation": "Annotation improvement",
        "priority": "High/Medium/Low",
        "impact": "High/Medium/Low"
      }}
    ],
    "enrichment_opportunities": [
      {{
        "recommendation": "Data enrichment opportunity",
        "priority": "High/Medium/Low",
        "impact": "High/Medium/Low"
      }}
    ]
  }},
  "infrastructure_suggestions": [
    {{
      "suggestion": "Infrastructure improvement",
      "category": "data_pipeline/analytics/storage",
      "priority": "High/Medium/Low",
      "effort": "High/Medium/Low"
    }}
  ],
  "data_enhancement_opportunities": [
    {{
      "opportunity": "Data enhancement opportunity",
      "type": "collection/processing/analysis",
      "expected_benefit": "High/Medium/Low",
      "implementation_effort": "High/Medium/Low"
    }}
  ],
  "confidence_score": 0.85
}}
```

## ANALYSIS GUIDELINES
- Focus on actionable data quality improvements
- Consider sample size adequacy for statistical significance
- Identify specific data collection and annotation gaps
- Provide infrastructure recommendations for better analytics
- Highlight opportunities for data enrichment and enhancement

Please analyze the data quality metrics and provide your insights."""
    
    return prompt

def analyze_data_quality(run_hash: str, run_metadata: Dict) -> Dict:
    """Analyze data quality using specialized LLM."""
    print("🔍 Analyzing data quality...")
    
    # Load data
    data = load_data_quality_metrics(run_hash)
    
    # Generate prompt
    prompt = generate_data_quality_prompt(data, run_metadata)
    
    # Call LLM
    insights = call_openai_api(prompt)
    
    # Add metadata
    insights['metadata'] = {
        'generated_at': datetime.now().isoformat(),
        'run_hash': run_hash,
        'analyst_type': 'data_quality',
        'data_files_used': ['schema_mapping.json', 'aggregation_summary.json', 'segmentation_summary.json']
    }
    
    return insights

def main():
    """Main function for testing."""
    print("🚀 Starting Data Quality & Improvement Analyst v1.0.0")
    print("=" * 80)
    
    run_hash = os.environ.get('RUN_HASH', 'test')
    run_metadata = {
        'run_hash': run_hash,
        'date_range': '2025-09-15 to 2025-09-30',
        'data_source': 'phase_3_fallback'
    }
    
    try:
        insights = analyze_data_quality(run_hash, run_metadata)
        print("✅ Data quality analysis completed!")
        print(f"📊 Insights: {json.dumps(insights, indent=2, default=str)}")
        return 0
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
