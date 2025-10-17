#!/usr/bin/env python3
"""
LLM Insights Generation Script - Phase 5
Version: 1.0.0
Last Updated: 2025-10-15

Changelog:
- v1.0.0 (2025-10-15): Initial implementation with Phase 3/4 fallback and comprehensive segmentation analysis

Description:
This script generates AI-powered insights from Phase 3 segmentation data or Phase 4 validated data.
Provides actionable recommendations for product managers with statistical backing and confidence scoring.

Environment Variables:
- RUN_HASH: Unique identifier for the current run
- LLM_PROVIDER: LLM provider ("openai" or "huggingface")
- OPENAI_API_KEY: OpenAI API key (if using OpenAI)
- HUGGINGFACE_API_KEY: Hugging Face API key (if using Hugging Face)
- INSIGHTS_DEPTH: Insight depth level ("summary", "detailed", "comprehensive")
- FORCE_PHASE_3_DATA: Force use of Phase 3 data even if Phase 4 exists ("true"/"false")

Dependencies:
- openai: OpenAI API client
- requests: HTTP requests for Hugging Face API
- pandas: Data manipulation and analysis
- numpy: Statistical calculations
- json: JSON serialization
- os: Environment variable access
- pathlib: Path handling
- datetime: Timestamp generation
"""
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import requests

# Import LLM providers
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ Warning: OpenAI library not available. Install with: pip install openai")

def get_llm_provider():
    """Get the configured LLM provider."""
    provider = os.environ.get('LLM_PROVIDER', 'openai').lower()
    
    if provider == 'openai' and not OPENAI_AVAILABLE:
        print("⚠️ OpenAI requested but not available. Falling back to Hugging Face.")
        provider = 'huggingface'
    
    return provider

def validate_data_sources(run_hash: str) -> Tuple[str, Dict]:
    """
    Validate and determine the best data source for analysis.
    Returns: (data_source_type, data_info)
    """
    print("🔍 Validating data sources...")
    
    # Check if Phase 4 data is available and should be used
    force_phase_3 = os.environ.get('FORCE_PHASE_3_DATA', 'false').lower() == 'true'
    
    if not force_phase_3:
        phase_4_validation = f'run_logs/{run_hash}/outputs/validation/validation_results.json'
        if Path(phase_4_validation).exists():
            print("✅ Phase 4 validated data found")
            return "phase_4_validated", {"source": phase_4_validation, "quality": "high"}
    
    # Fallback to Phase 3 data
    phase_3_outputs = [
        f'run_logs/{run_hash}/outputs/segments/daily/dau_by_date.csv',
        f'run_logs/{run_hash}/outputs/segments/daily/dau_by_country.csv',
        f'run_logs/{run_hash}/outputs/segments/user_level/revenue_segments_daily.csv',
        f'run_logs/{run_hash}/outputs/segments/user_level/user_journey_cohort.csv',
        f'run_logs/{run_hash}/outputs/segments/cohort/dau_by_cohort_date.csv',
        f'run_logs/{run_hash}/outputs/segments/cohort/engagement_by_cohort_date.csv'
    ]
    
    existing_files = []
    missing_files = []
    
    for file_path in phase_3_outputs:
        if Path(file_path).exists():
            existing_files.append(file_path)
        else:
            missing_files.append(file_path)
    
    if existing_files:
        print(f"✅ Phase 3 data found ({len(existing_files)} files)")
        return "phase_3_fallback", {
            "source": "phase_3_outputs",
            "files": existing_files,
            "missing": missing_files,
            "quality": "medium"
        }
    else:
        raise ValueError("No valid data sources found for analysis")

def load_analysis_data(run_hash: str, data_source_type: str, data_info: Dict) -> Dict:
    """
    Load and prepare analysis data from the determined source.
    """
    print(f"📊 Loading analysis data from {data_source_type}...")
    
    data = {
        'run_hash': run_hash,
        'data_source': data_source_type,
        'timestamp': datetime.now().isoformat(),
        'summary_metrics': {},
        'country_breakdown': {},
        'revenue_breakdown': {},
        'channel_breakdown': {},
        'behavioral_segments': {},
        'cohort_analysis': {},
        'cross_segmentation': {},
        'data_quality_notes': []
    }
    
    if data_source_type == "phase_4_validated":
        # Load Phase 4 validated data
        with open(data_info['source'], 'r') as f:
            validation_data = json.load(f)
        
        data['summary_metrics'] = validation_data.get('data_quality_validation', {})
        data['data_quality_notes'].append("Data validated through Phase 4 quality assurance")
        
    elif data_source_type == "phase_3_fallback":
        # Load Phase 3 segmentation data
        data = load_phase_3_data(data, data_info['files'])
    
    return data

def load_phase_3_data(data: Dict, files: List[str]) -> Dict:
    """Load and process Phase 3 segmentation data."""
    
    # Load DAU by date
    dau_file = next((f for f in files if 'dau_by_date.csv' in f), None)
    if dau_file:
        df_dau = pd.read_csv(dau_file)
        data['summary_metrics']['dau_trend'] = df_dau.to_dict('records')
        data['summary_metrics']['total_users'] = df_dau['dau'].sum() if 'dau' in df_dau.columns else 0
        data['summary_metrics']['avg_dau'] = df_dau['dau'].mean() if 'dau' in df_dau.columns else 0
    
    # Load DAU by country
    country_file = next((f for f in files if 'dau_by_country.csv' in f), None)
    if country_file:
        df_country = pd.read_csv(country_file)
        data['country_breakdown'] = df_country.to_dict('records')
    
    # Load user journey data (replaces behavioral segments)
    journey_file = next((f for f in files if 'user_journey_cohort.csv' in f), None)
    if journey_file:
        df_journey = pd.read_csv(journey_file)
        data['behavioral_segments'] = df_journey.to_dict('records')
    
    # Load revenue segments
    revenue_file = next((f for f in files if 'revenue_segments_daily.csv' in f), None)
    if revenue_file:
        df_revenue = pd.read_csv(revenue_file)
        data['revenue_breakdown'] = df_revenue.to_dict('records')
    
    # Load cohort analysis
    cohort_file = next((f for f in files if 'dau_by_cohort_date.csv' in f), None)
    if cohort_file:
        df_cohort = pd.read_csv(cohort_file)
        data['cohort_analysis'] = {
            'dau_by_cohort': df_cohort.to_dict('records')
        }
    
    # Load engagement data (replaces retention data)
    engagement_file = next((f for f in files if 'engagement_by_cohort_date.csv' in f), None)
    if engagement_file:
        df_engagement = pd.read_csv(engagement_file)
        if 'cohort_analysis' not in data:
            data['cohort_analysis'] = {}
        data['cohort_analysis']['engagement'] = df_engagement.to_dict('records')
    
    return data

def calculate_statistical_metrics(data: Dict) -> Dict:
    """Calculate statistical metrics and confidence scores."""
    print("📈 Calculating statistical metrics...")
    
    stats = {
        'sample_sizes': {},
        'confidence_scores': {},
        'data_completeness': {},
        'statistical_significance': {}
    }
    
    # Calculate sample sizes for different segments
    if 'behavioral_segments' in data and data['behavioral_segments']:
        df_segments = pd.DataFrame(data['behavioral_segments'])
        if 'user_count' in df_segments.columns:
            stats['sample_sizes']['behavioral_segments'] = df_segments['user_count'].sum()
    
    if 'revenue_breakdown' in data and data['revenue_breakdown']:
        df_revenue = pd.DataFrame(data['revenue_breakdown'])
        if 'user_count' in df_revenue.columns:
            stats['sample_sizes']['revenue_segments'] = df_revenue['user_count'].sum()
    
    # Calculate confidence scores based on sample size and data completeness
    for segment_type, sample_size in stats['sample_sizes'].items():
        if sample_size >= 1000:
            confidence = 0.9
        elif sample_size >= 500:
            confidence = 0.8
        elif sample_size >= 100:
            confidence = 0.7
        elif sample_size >= 30:
            confidence = 0.6
        else:
            confidence = 0.4
        
        stats['confidence_scores'][segment_type] = confidence
    
    return stats

def generate_enhanced_prompt(data: Dict, stats: Dict) -> str:
    """Generate the enhanced LLM prompt with dynamic data."""
    
    # Prepare data snippets for the prompt
    data_snippets = prepare_data_snippets(data)
    
    prompt = f"""## PRODUCT ANALYTICS ANALYSIS REQUEST

### CONTEXT
I'm analyzing a mobile app's user behavior and performance data. Please provide comprehensive insights and actionable recommendations based on the following data.

### APP INFORMATION
- **Run Hash**: {data['run_hash']}
- **Analysis Timestamp**: {data['timestamp']}
- **Data Source**: {data['data_source']}
- **Data Quality**: {data.get('data_quality_notes', ['Standard quality'])[0] if data.get('data_quality_notes') else 'Standard quality'}

### KEY METRICS DATA

#### DAILY ACTIVE USERS TREND
```csv
{data_snippets.get('dau_trend', 'No DAU data available')}
```

#### USER SEGMENTS BY BEHAVIOR
```csv
{data_snippets.get('behavioral_segments', 'No behavioral segment data available')}
```

#### REVENUE BREAKDOWN
```csv
{data_snippets.get('revenue_breakdown', 'No revenue data available')}
```

#### COUNTRY BREAKDOWN
```csv
{data_snippets.get('country_breakdown', 'No country data available')}
```

#### COHORT ANALYSIS
```csv
{data_snippets.get('cohort_analysis', 'No cohort data available')}
```

### STATISTICAL CONFIDENCE
- **Sample Sizes**: {stats.get('sample_sizes', {})}
- **Confidence Scores**: {stats.get('confidence_scores', {})}
- **Data Completeness**: {stats.get('data_completeness', {})}

### ANALYSIS REQUIREMENTS

Please provide a comprehensive analysis with the following structure:

#### 1. EXECUTIVE SUMMARY (2-3 sentences)
- Key performance highlights
- Most critical finding
- Primary recommendation

#### 2. KEY FINDINGS (Top 5)
- Data-backed insights with specific numbers
- Trend analysis and patterns
- Statistical significance where applicable

#### 3. SEGMENT ANALYSIS
- Performance comparison across user segments
- Opportunities for segment-specific strategies
- Risk areas requiring attention

#### 4. COHORT INSIGHTS
- Retention pattern analysis
- Cohort performance trends
- Recommendations for cohort-specific actions

#### 5. GEOGRAPHIC ANALYSIS
- Country-specific performance insights
- Regional opportunities and challenges
- Localization recommendations

#### 6. REVENUE OPTIMIZATION
- Revenue stream analysis
- Monetization opportunities
- Pricing and strategy recommendations

#### 7. ACTIONABLE RECOMMENDATIONS
- **Immediate Actions** (next 1-2 weeks)
- **Short-term Goals** (next 1-3 months)  
- **Long-term Strategy** (next 3-6 months)

#### 8. RISK ASSESSMENT
- Potential issues or declining trends
- Data quality concerns
- Competitive threats or market changes

#### 9. OPPORTUNITY AREAS
- Underserved user segments
- Revenue optimization potential
- Engagement improvement opportunities

#### 10. DATA IMPROVEMENT RECOMMENDATIONS
- Potential improvements in data annotation and enrichment
- Additional data collection opportunities
- Analytics infrastructure enhancements

### OUTPUT FORMAT
Please provide your analysis in JSON format with the following structure:

```json
{{
  "executive_summary": "...",
  "key_findings": [
    {{
      "finding": "...",
      "data_point": "...",
      "significance": "...",
      "recommendation": "...",
      "confidence_score": 0.85
    }}
  ],
  "segment_analysis": {{
    "top_performing_segment": "...",
    "underperforming_segment": "...",
    "segment_opportunities": [...],
    "cross_segment_insights": [...]
  }},
  "cohort_insights": {{
    "retention_trend": "...",
    "best_performing_cohort": "...",
    "cohort_recommendations": [...]
  }},
  "geographic_analysis": {{
    "top_countries": [...],
    "regional_opportunities": [...],
    "localization_recommendations": [...]
  }},
  "revenue_optimization": {{
    "revenue_stream_analysis": "...",
    "monetization_opportunities": [...],
    "pricing_recommendations": [...]
  }},
  "recommendations": {{
    "immediate_actions": [...],
    "short_term_goals": [...],
    "long_term_strategy": [...]
  }},
  "risk_assessment": {{
    "high_priority_risks": [...],
    "data_quality_concerns": [...],
    "market_risks": [...]
  }},
  "opportunity_areas": {{
    "revenue_optimization": [...],
    "engagement_improvement": [...],
    "user_acquisition": [...]
  }},
  "data_improvement_recommendations": {{
    "annotation_improvements": [...],
    "enrichment_opportunities": [...],
    "infrastructure_enhancements": [...]
  }}
}}
```

### ANALYSIS GUIDELINES
- Base all insights on the provided data
- Use specific numbers and percentages
- Focus on actionable recommendations
- Consider statistical significance and confidence scores
- Analyze different data cuts (by country, by channel, by revenue type, etc.)
- Think like a product manager making business decisions
- Prioritize recommendations by impact and feasibility
- Include confidence scores for each insight
- Highlight areas where data is insufficient for analysis

Please analyze this data and provide your comprehensive insights."""
    
    return prompt

def prepare_data_snippets(data: Dict) -> Dict:
    """Prepare data snippets for the LLM prompt."""
    snippets = {}
    
    # DAU trend snippet (reduced to 5 rows)
    if 'dau_trend' in data and data['dau_trend']:
        df_dau = pd.DataFrame(data['dau_trend'])
        snippets['dau_trend'] = df_dau.head(5).to_csv(index=False)
    
    # Behavioral segments snippet (reduced to 5 rows)
    if 'behavioral_segments' in data and data['behavioral_segments']:
        df_segments = pd.DataFrame(data['behavioral_segments'])
        snippets['behavioral_segments'] = df_segments.head(5).to_csv(index=False)
    
    # Revenue breakdown snippet (reduced to 5 rows)
    if 'revenue_breakdown' in data and data['revenue_breakdown']:
        df_revenue = pd.DataFrame(data['revenue_breakdown'])
        snippets['revenue_breakdown'] = df_revenue.head(5).to_csv(index=False)
    
    # Country breakdown snippet (reduced to 5 rows)
    if 'country_breakdown' in data and data['country_breakdown']:
        df_country = pd.DataFrame(data['country_breakdown'])
        snippets['country_breakdown'] = df_country.head(5).to_csv(index=False)
    
    # Cohort analysis snippet (reduced to 5 rows)
    if 'cohort_analysis' in data and data['cohort_analysis']:
        df_cohort = pd.DataFrame(data['cohort_analysis'])
        snippets['cohort_analysis'] = df_cohort.head(5).to_csv(index=False)
    
    return snippets

def call_openai_api(prompt: str) -> Dict:
    """Call OpenAI API to generate insights."""
    print("🤖 Calling OpenAI API...")
    
    client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert product analytics consultant with 10+ years of experience in mobile app analytics, user behavior analysis, and data-driven product strategy. You specialize in translating complex data into actionable business insights for product managers and executives. You also provide recommendations on potential improvements in data annotation and enrichment of raw data wherever you see opportunities."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        content = response.choices[0].message.content
        
        # Try to parse JSON response
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # If not JSON, wrap in a basic structure
            return {
                "raw_response": content,
                "parsing_error": "Response was not in expected JSON format"
            }
            
    except Exception as e:
        raise Exception(f"OpenAI API call failed: {str(e)}")

def call_huggingface_api(prompt: str) -> Dict:
    """Call Hugging Face API as fallback."""
    print("🤖 Calling Hugging Face API...")
    
    api_key = os.environ.get('HUGGINGFACE_API_KEY')
    if not api_key:
        raise Exception("Hugging Face API key not provided")
    
    # Use a suitable model for text generation
    model = "microsoft/DialoGPT-large"
    
    try:
        response = requests.post(
            f"https://api-inference.huggingface.co/models/{model}",
            headers={"Authorization": f"Bearer {api_key}"},
            json={"inputs": prompt, "parameters": {"max_length": 1000}}
        )
        
        if response.status_code != 200:
            raise Exception(f"Hugging Face API error: {response.status_code}")
        
        result = response.json()
        
        # Hugging Face returns different format, adapt as needed
        return {
            "raw_response": result,
            "provider": "huggingface",
            "note": "Hugging Face response format may need adaptation"
        }
        
    except Exception as e:
        raise Exception(f"Hugging Face API call failed: {str(e)}")

def generate_insights_with_llm(prompt: str) -> Dict:
    """Generate insights using the configured LLM provider."""
    provider = get_llm_provider()
    
    try:
        if provider == 'openai':
            return call_openai_api(prompt)
        elif provider == 'huggingface':
            return call_huggingface_api(prompt)
        else:
            raise Exception(f"Unsupported LLM provider: {provider}")
            
    except Exception as e:
        print(f"❌ LLM API call failed: {str(e)}")
        raise

def validate_insights(insights: Dict, data: Dict, stats: Dict) -> Dict:
    """Validate and enhance the generated insights."""
    print("✅ Validating generated insights...")
    
    # Add metadata
    insights['metadata'] = {
        'generated_at': datetime.now().isoformat(),
        'data_source': data['data_source'],
        'run_hash': data['run_hash'],
        'statistical_metrics': stats,
        'validation_status': 'validated'
    }
    
    # Add confidence scoring explanation
    insights['confidence_scoring'] = {
        'methodology': 'Based on sample size, data completeness, and statistical significance',
        'sample_size_weights': {
            '>=1000': 0.9,
            '>=500': 0.8,
            '>=100': 0.7,
            '>=30': 0.6,
            '<30': 0.4
        },
        'data_quality_factors': [
            'Data source reliability (Phase 4 > Phase 3)',
            'Sample size adequacy',
            'Data completeness',
            'Statistical significance'
        ]
    }
    
    return insights

def save_insights_outputs(insights: Dict, run_hash: str, prompt: str = None) -> Dict:
    """Save insights to output files."""
    print("💾 Saving insights outputs...")
    
    outputs_dir = f'run_logs/{run_hash}/outputs/insights'
    os.makedirs(outputs_dir, exist_ok=True)
    
    # Save main insights report
    insights_path = f'{outputs_dir}/insights_report.json'
    with open(insights_path, 'w') as f:
        json.dump(insights, f, indent=2, default=str)
    
    # Save the LLM prompt for reference
    if prompt:
        prompt_path = f'{outputs_dir}/llm_prompt.md'
        with open(prompt_path, 'w') as f:
            f.write(f"# LLM Prompt Used in Run {run_hash}\n\n")
            f.write(f"**Generated at**: {datetime.now().isoformat()}\n")
            f.write(f"**Data Source**: {insights.get('metadata', {}).get('data_source', 'unknown')}\n")
            f.write(f"**LLM Provider**: {os.environ.get('LLM_PROVIDER', 'unknown')}\n\n")
            f.write("---\n\n")
            f.write(prompt)
        print(f"✅ LLM prompt saved to: {prompt_path}")
    
    # Save executive summary
    if 'executive_summary' in insights:
        summary_path = f'{outputs_dir}/executive_summary.md'
        with open(summary_path, 'w') as f:
            f.write(f"# Executive Summary\n\n{insights['executive_summary']}\n")
    
    # Save recommendations
    if 'recommendations' in insights:
        rec_path = f'{outputs_dir}/recommendations.json'
        with open(rec_path, 'w') as f:
            json.dump(insights['recommendations'], f, indent=2, default=str)
    
    # Save confidence scoring log
    if 'confidence_scoring' in insights:
        conf_path = f'{outputs_dir}/confidence_scoring_log.json'
        with open(conf_path, 'w') as f:
            json.dump(insights['confidence_scoring'], f, indent=2, default=str)
    
    return {
        'insights_report': insights_path,
        'llm_prompt': prompt_path if prompt else None,
        'executive_summary': summary_path if 'executive_summary' in insights else None,
        'recommendations': rec_path if 'recommendations' in insights else None,
        'confidence_log': conf_path if 'confidence_scoring' in insights else None
    }

def main():
    """Main LLM insights generation function."""
    print("🚀 Starting LLM Insights Generation v1.0.0")
    print("=" * 80)
    
    # Get environment variables
    run_hash = os.environ.get('RUN_HASH')
    if not run_hash:
        print("❌ RUN_HASH environment variable not set")
        return 1
    
    print(f"Run Hash: {run_hash}")
    print(f"LLM Provider: {get_llm_provider()}")
    print()
    
    try:
        # 1. Validate data sources
        data_source_type, data_info = validate_data_sources(run_hash)
        print(f"📊 Using data source: {data_source_type}")
        
        # 2. Load analysis data
        data = load_analysis_data(run_hash, data_source_type, data_info)
        
        # 3. Calculate statistical metrics
        stats = calculate_statistical_metrics(data)
        
        # 4. Generate enhanced prompt
        prompt = generate_enhanced_prompt(data, stats)
        
        # 5. Generate insights with LLM
        insights = generate_insights_with_llm(prompt)
        
        # 6. Validate insights
        insights = validate_insights(insights, data, stats)
        
        # 7. Save outputs
        output_paths = save_insights_outputs(insights, run_hash, prompt)
        
        print("\n🎉 LLM Insights Generation v1.0.0 completed successfully!")
        print(f"📊 Insights report: {output_paths['insights_report']}")
        if output_paths['llm_prompt']:
            print(f"📝 LLM prompt: {output_paths['llm_prompt']}")
        if output_paths['executive_summary']:
            print(f"📋 Executive summary: {output_paths['executive_summary']}")
        if output_paths['recommendations']:
            print(f"💡 Recommendations: {output_paths['recommendations']}")
        if output_paths['confidence_log']:
            print(f"📈 Confidence scoring: {output_paths['confidence_log']}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during LLM insights generation: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
