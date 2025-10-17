#!/usr/bin/env python3
"""
LLM Coordinator - Parent LLM for Multi-LLM Architecture
Version: 1.0.0
Last Updated: 2025-10-16

Description:
Parent LLM that coordinates insights from multiple specialized child LLMs
and synthesizes them into a comprehensive executive report.

Dependencies:
- openai: OpenAI API client
- json: JSON serialization
- os: Environment variable access
- datetime: Timestamp generation
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
    """Call OpenAI API to generate coordinated insights."""
    print("🤖 Calling OpenAI API for coordination...")
    
    client = openai.OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a senior product analytics director with 15+ years of experience in mobile app analytics and strategic decision-making. You excel at synthesizing complex data from multiple specialized analysts into clear, actionable executive insights. You provide strategic recommendations that drive business growth and product optimization."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2,
            max_tokens=2000
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

def generate_coordination_prompt(child_outputs: Dict, run_metadata: Dict) -> str:
    """Generate the coordination prompt for the parent LLM."""
    
    # Extract key insights from each child output to reduce token usage
    def extract_key_insights(output, analyst_name):
        if "error" in output:
            return f"{analyst_name}: Error - {output.get('error', 'Unknown error')}"
        
        # Try to extract key insights from the output
        if "trend_analysis" in output:
            return f"{analyst_name}: {output.get('trend_analysis', {}).get('dau_trend', 'Unknown trend')}"
        elif "segment_analysis" in output:
            return f"{analyst_name}: Top segment - {output.get('segment_analysis', {}).get('top_performing_segment', 'Unknown')}"
        elif "market_analysis" in output:
            return f"{analyst_name}: Primary market - {output.get('market_analysis', {}).get('primary_markets', ['Unknown'])[0]}"
        elif "retention_analysis" in output:
            return f"{analyst_name}: Retention health - {output.get('retention_analysis', {}).get('retention_health_score', 'Unknown')}"
        elif "revenue_analysis" in output:
            return f"{analyst_name}: Monetization rate - {output.get('revenue_analysis', {}).get('monetization_rate', 'Unknown')}"
        elif "quality_assessment" in output:
            return f"{analyst_name}: Quality score - {output.get('quality_assessment', {}).get('overall_quality_score', 'Unknown')}"
        else:
            return f"{analyst_name}: Analysis completed"
    
    # Create concise summaries
    daily_insights = extract_key_insights(child_outputs.get('daily_metrics', {}), "Daily Metrics")
    segmentation_insights = extract_key_insights(child_outputs.get('user_segmentation', {}), "User Segmentation")
    geographic_insights = extract_key_insights(child_outputs.get('geographic', {}), "Geographic")
    cohort_insights = extract_key_insights(child_outputs.get('cohort_retention', {}), "Cohort & Retention")
    revenue_insights = extract_key_insights(child_outputs.get('revenue_optimization', {}), "Revenue Optimization")
    quality_insights = extract_key_insights(child_outputs.get('data_quality', {}), "Data Quality")
    
    prompt = f"""# EXECUTIVE ANALYTICS SYNTHESIS

## CONTEXT
You are a senior product analytics director. Synthesize insights from 6 specialized analysts into an executive report.

## RUN INFO
- Run: {run_metadata.get('run_hash', 'unknown')}
- Period: {run_metadata.get('date_range', 'unknown')}
- Users: {run_metadata.get('total_users', 'unknown')}

## ANALYST INSIGHTS
- {daily_insights}
- {segmentation_insights}
- {geographic_insights}
- {cohort_insights}
- {revenue_insights}
- {quality_insights}

## TASK
Create an executive synthesis with:
1. Executive Summary (2-3 sentences)
2. Top 3 Strategic Insights
3. Priority Recommendations (Immediate/Short-term/Long-term)
4. Key Risks & Opportunities
5. Implementation Roadmap

## OUTPUT FORMAT
```json
{{
  "executive_summary": "Brief summary of key findings and primary recommendation",
  "strategic_insights": [
    {{"insight": "Key insight", "impact": "High/Medium/Low", "confidence": 0.85}}
  ],
  "priority_recommendations": {{
    "immediate": [{{"action": "Action", "timeline": "1-2 weeks", "impact": "High"}}],
    "short_term": [{{"action": "Action", "timeline": "1-3 months", "impact": "Medium"}}],
    "long_term": [{{"action": "Action", "timeline": "3-6 months", "impact": "High"}}]
  }},
  "risk_assessment": [{{"risk": "Risk description", "severity": "High", "mitigation": "Strategy"}}],
  "opportunity_matrix": [{{"opportunity": "Opportunity", "impact": "High", "effort": "Low"}}],
  "implementation_roadmap": {{
    "phase_1": {{"duration": "1-2 weeks", "actions": ["Action 1", "Action 2"]}},
    "phase_2": {{"duration": "1-3 months", "actions": ["Action 1", "Action 2"]}},
    "phase_3": {{"duration": "3-6 months", "actions": ["Action 1", "Action 2"]}}
  }},
  "confidence_score": 0.85
}}
```

Provide actionable insights for executive decision-making."""
    
    return prompt

def coordinate_insights(child_outputs: Dict, run_metadata: Dict) -> Dict:
    """Coordinate insights from child LLMs using parent LLM."""
    print("🎯 Coordinating insights from child LLMs...")
    
    # Generate coordination prompt
    prompt = generate_coordination_prompt(child_outputs, run_metadata)
    
    # Call OpenAI API
    coordinated_insights = call_openai_api(prompt)
    
    # Add metadata
    coordinated_insights['metadata'] = {
        'generated_at': datetime.now().isoformat(),
        'run_hash': run_metadata.get('run_hash', 'unknown'),
        'data_source': run_metadata.get('data_source', 'unknown'),
        'coordination_method': 'multi_llm_architecture',
        'child_analysts_count': len(child_outputs),
        'validation_status': 'coordinated'
    }
    
    return coordinated_insights

def main():
    """Main coordination function for testing."""
    print("🚀 Starting LLM Coordinator v1.0.0")
    print("=" * 80)
    
    # Test data
    child_outputs = {
        'daily_metrics': {
            'trend_analysis': {'dau_trend': 'growing', 'growth_rate': '15%'},
            'key_metrics': {'avg_dau': 650, 'new_user_ratio': 0.45}
        },
        'user_segmentation': {
            'segment_analysis': {'top_performing_segment': 'engaged_users'},
            'journey_insights': {'common_paths': ['onboarding', 'first_action']}
        },
        'geographic': {
            'market_analysis': {'primary_markets': ['India'], 'market_concentration': 0.95}
        },
        'cohort_retention': {
            'retention_analysis': {'retention_health_score': 0.75}
        },
        'revenue_optimization': {
            'revenue_analysis': {'monetization_rate': 0.15}
        },
        'data_quality': {
            'quality_assessment': {'overall_quality_score': 0.85}
        }
    }
    
    run_metadata = {
        'run_hash': 'test_coordinator',
        'date_range': '2025-09-15 to 2025-09-30',
        'data_source': 'phase_3_fallback',
        'total_users': 10000,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        # Coordinate insights
        coordinated_insights = coordinate_insights(child_outputs, run_metadata)
        
        print("✅ Coordination completed successfully!")
        print(f"📊 Coordinated insights: {json.dumps(coordinated_insights, indent=2, default=str)}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during coordination: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
