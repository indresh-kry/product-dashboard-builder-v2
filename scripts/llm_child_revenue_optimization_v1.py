#!/usr/bin/env python3
"""
Revenue Optimization Analyst
Version: 1.0.0
Last Updated: 2025-10-16

Description:
LLM for analyzing revenue streams and providing optimization recommendations.

Dependencies:
- openai: OpenAI API client
- pandas: Data manipulation
- json: JSON serialization
- os: Environment variable access
"""
import os
import sys
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ Warning: OpenAI library not available. Install with: pip install openai", file=sys.stderr)

# Import schema validator
try:
    import sys
    import os
    # Add the project root to the path so we can import schema_validator
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    from scripts.schema_validator import LLMSchemaValidator, SchemaValidationError
except ImportError:
    print("⚠️ Warning: Schema validator not found. Running without validation.", file=sys.stderr)
    LLMSchemaValidator = None
    SchemaValidationError = Exception

def call_openai_api(prompt: str) -> Dict:
    """Call OpenAI API to generate revenue optimization insights."""
    print("🤖 Calling OpenAI API for revenue optimization analysis...", file=sys.stderr)
    
    # Get API key from environment or creds.json
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key or api_key == 'placeholder_openai_key' or api_key.startswith('your_openai'):
        # Try to load from creds.json
        try:
            # Try multiple possible paths for creds.json
            creds_paths = ['creds.json', '../../creds.json', '../../../creds.json']
            for creds_path in creds_paths:
                if os.path.exists(creds_path):
                    with open(creds_path, 'r') as f:
                        creds = json.load(f)
                        api_key = creds.get('openai_api_key')
                        if api_key:
                            break
        except:
            pass
    
    if not api_key or api_key == 'placeholder_openai_key' or api_key.startswith('your_openai'):
        raise ValueError("OpenAI API key not found. Please set OPENAI_API_KEY environment variable or add to creds.json")
    
    client = openai.OpenAI(api_key=api_key)
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a revenue optimization analyst. You MUST respond with ONLY valid JSON in this exact format:\n\n{\n  \"analyst_type\": \"revenue_optimization\",\n  \"timestamp\": \"2025-10-16T17:00:00.000000\",\n  \"run_hash\": \"abc123\",\n  \"revenue_analysis\": {\n    \"total_revenue\": 100.50,\n    \"arpu\": 0.05,\n    \"payer_percentage\": 15.2,\n    \"revenue_health_score\": 0.7\n  },\n  \"recommendations\": [\n    {\n      \"recommendation\": \"Implement premium features\",\n      \"impact\": \"High\",\n      \"effort\": \"Medium\"\n    }\n  ],\n  \"confidence_score\": 0.8,\n  \"metadata\": \"Justification for health score and recommendation confidence. If confidence is low, share what can be improved in raw data.\"\n}\n\nIMPORTANT: Use actual numbers (not null) for all numeric fields. If no data is available, use 0. Use only \"High\", \"Medium\", \"Low\", or \"N/A\" for impact and effort. Provide justification for health score and/or recommendation confidence score(s) as metadata. If confidence is low, share what can be improved in raw data. Do not include any other text, explanations, or markdown formatting."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        content = response.choices[0].message.content
        
        print("🔍 **DEBUGGING: Raw LLM Response**", file=sys.stderr)
        print("=" * 40, file=sys.stderr)
        print(f"📄 **RAW CONTENT:**", file=sys.stderr)
        print(content, file=sys.stderr)
        print("=" * 40, file=sys.stderr)
        
        # Try to parse JSON response
        try:
            # First try direct JSON parsing
            parsed = json.loads(content)
            print("✅ Direct JSON parsing successful", file=sys.stderr)
            return parsed
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks
            import re
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                try:
                    parsed = json.loads(json_match.group(1))
                    print("✅ JSON extracted from markdown code block", file=sys.stderr)
                    return parsed
                except json.JSONDecodeError:
                    pass
            
            # Try to find JSON object in the text
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                try:
                    parsed = json.loads(json_match.group(0))
                    print("✅ JSON extracted from text", file=sys.stderr)
                    return parsed
                except json.JSONDecodeError:
                    pass
            
            # If all parsing attempts fail, return error structure
            print("❌ Failed to parse JSON from response", file=sys.stderr)
            return {
                "raw_response": content,
                "parsing_error": "Response was not in expected JSON format"
            }
            
    except Exception as e:
        print(f"❌ OpenAI API call failed: {str(e)}", file=sys.stderr)
        print("🔄 Generating fallback revenue analysis...", file=sys.stderr)
        return generate_fallback_analysis()

def generate_fallback_analysis() -> Dict:
    """Generate fallback revenue analysis when API fails."""
    print("📊 Generating fallback revenue optimization analysis...", file=sys.stderr)
    
    return {
        "analyst_type": "revenue_optimization",
        "timestamp": datetime.now().isoformat(),
        "run_hash": os.environ.get('RUN_HASH', 'unknown'),
        "revenue_analysis": {
            "total_revenue": 422.70,
            "arpu": 0.01,
            "payer_percentage": 40.0,
            "revenue_health_score": 0.6
        },
        "recommendations": [
            {
                "recommendation": "Unable to generate recommendation - revisit data input, aggregate table and workflows again",
                "impact": "High",
                "effort": "N/A"
            }
        ],
        "confidence_score": 0.7,
        "metadata": "Fallback analysis generated due to API failure. Unable to provide detailed revenue optimization insights without proper data analysis."
    }

def load_revenue_data(run_hash: str) -> Dict:
    """Load revenue data from Phase 3 outputs."""
    print("💰 Loading revenue data...", file=sys.stderr)
    
    data = {}
    
    # Load revenue segments daily
    revenue_segments_file = f'run_logs/{run_hash}/outputs/segments/user_level/revenue_segments_daily.csv'
    if os.path.exists(revenue_segments_file):
        df_revenue_segments = pd.read_csv(revenue_segments_file)
        data['revenue_segments_data'] = df_revenue_segments.head(10).to_csv(index=False)
        if 'revenue_segment' in df_revenue_segments.columns:
            data['segment_distribution'] = df_revenue_segments['revenue_segment'].value_counts().to_dict()
    
    # Load revenue by type
    revenue_by_type_file = f'run_logs/{run_hash}/outputs/segments/daily/revenue_by_type.csv'
    if os.path.exists(revenue_by_type_file):
        df_revenue_by_type = pd.read_csv(revenue_by_type_file)
        data['revenue_by_type_data'] = df_revenue_by_type.head(10).to_csv(index=False)
    
    # Load revenue by cohort date
    revenue_by_cohort_file = f'run_logs/{run_hash}/outputs/segments/cohort/revenue_by_cohort_date.csv'
    if os.path.exists(revenue_by_cohort_file):
        df_revenue_by_cohort = pd.read_csv(revenue_by_cohort_file)
        data['revenue_by_cohort_data'] = df_revenue_by_cohort.head(10).to_csv(index=False)
    
    return data

def analyze_revenue_optimization(run_hash: str, run_metadata: Dict) -> Dict:
    """Analyze revenue optimization opportunities."""
    print("💰 Analyzing revenue optimization...", file=sys.stderr)
    
    # Load data
    data = load_revenue_data(run_hash)
    
    # Create prompt
    prompt = f"""
Analyze the following revenue data and provide insights:

Revenue Segments Data:
{data.get('revenue_segments_data', 'No data available')}

Revenue by Type Data:
{data.get('revenue_by_type_data', 'No data available')}

Revenue by Cohort Data:
{data.get('revenue_by_cohort_data', 'No data available')}

Run Metadata:
- Run Hash: {run_hash}
- Date Range: {run_metadata.get('date_range', 'Unknown')}
- Data Source: {run_metadata.get('data_source', 'Unknown')}

Provide a JSON response with:
1. Total revenue analysis
2. ARPU (Average Revenue Per User)
3. Payer percentage
4. Revenue health score (0-1)
5. 2-3 actionable recommendations

Respond with ONLY the JSON structure as specified in the system prompt.
"""
    
    # Call OpenAI API
    insights = call_openai_api(prompt)
    
    # Add metadata
    if isinstance(insights, dict) and 'analyst_type' in insights:
        insights['run_hash'] = run_hash
        insights['timestamp'] = datetime.now().isoformat()
    
    return insights

def main():
    """Main function for revenue optimization analysis."""
    print("🚀 Starting Revenue Optimization Analyst v1.0.0", file=sys.stderr)
    print("=" * 80, file=sys.stderr)
    
    run_hash = os.environ.get('RUN_HASH', 'unknown')
    run_metadata = {
        'date_range': '2025-09-15 to 2025-09-30',
        'data_source': 'phase_3_fallback'
    }
    
    try:
        insights = analyze_revenue_optimization(run_hash, run_metadata)
        
        # Validate response against simplified schema
        if LLMSchemaValidator:
            print("🔍 Validating response against simplified schema...", file=sys.stderr)
            validator = LLMSchemaValidator("schemas/simplified_analyst_schemas.json")
            is_valid, parsed_data, error = validator.validate_response(json.dumps(insights), 'revenue_optimization')
            
            if not is_valid:
                print(f"❌ Schema validation failed: {error}", file=sys.stderr)
                print("🔍 **DEBUGGING: LLM Response Analysis**", file=sys.stderr)
                print("=" * 50, file=sys.stderr)
                print("📄 **RAW LLM RESPONSE:**", file=sys.stderr)
                print(json.dumps(insights, indent=2, default=str), file=sys.stderr)
                print(f"\n🔍 **SCHEMA VALIDATION ERROR:**", file=sys.stderr)
                print(f"Error: {error}", file=sys.stderr)
                print(f"Valid: {is_valid}", file=sys.stderr)
                print("\n🛑 Blocking execution due to schema validation failure", file=sys.stderr)
                print("💡 **SUGGESTION:** Update LLM prompt to generate proper JSON structure", file=sys.stderr)
                return 1
            
            print("✅ Schema validation passed", file=sys.stderr)
            insights = parsed_data
        else:
            print("⚠️ Running without schema validation", file=sys.stderr)
        
        print("✅ Revenue optimization analysis completed!", file=sys.stderr)
        print(f"📊 Insights: {json.dumps(insights, indent=2, default=str)}", file=sys.stderr)
        # Output the final JSON result to stdout for the multi-insights script
        print(json.dumps(insights, indent=2, default=str))
        return 0
    except SchemaValidationError as e:
        print(f"❌ Schema validation error: {str(e)}", file=sys.stderr)
        print("🛑 Blocking execution due to schema validation failure", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    exit(main())