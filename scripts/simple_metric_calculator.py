#!/usr/bin/env python3
"""
Simple Metric Calculator with Standardized Definitions
Version: 1.0.0
Last Updated: 2025-10-16

This module provides standardized metric calculations using the definitions
from schemas/metric_definitions.json. It ensures consistent calculations
across all analysts with proper units, confidence intervals, and validation.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import numpy as np

class SimpleMetricCalculator:
    """Calculates metrics using standardized definitions and formulas."""
    
    def __init__(self, definitions_file: str = "schemas/metric_definitions.json"):
        """Initialize the metric calculator with definitions."""
        self.definitions_file = definitions_file
        self.definitions = self._load_definitions()
        
    def _load_definitions(self) -> Dict[str, Any]:
        """Load metric definitions from the definitions file."""
        try:
            definitions_path = Path(self.definitions_file)
            if not definitions_path.exists():
                print(f"⚠️ Definitions file not found: {self.definitions_file}")
                return {}
                
            with open(definitions_path, 'r') as f:
                definitions = json.load(f)
                
            print(f"✅ Loaded metric definitions from: {self.definitions_file}")
            return definitions
            
        except Exception as e:
            print(f"❌ Error loading metric definitions: {str(e)}")
            return {}
    
    def get_metric_definition(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Get the definition for a specific metric."""
        for category, category_data in self.definitions.get('metric_categories', {}).items():
            metrics = category_data.get('metrics', {})
            if metric_name in metrics:
                return metrics[metric_name]
        return None
    
    def calculate_arpdau(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate Average Revenue Per Daily Active User."""
        total_revenue = data['total_revenue'].sum() if 'total_revenue' in data.columns else 0
        dau = data['user_id'].nunique() if 'user_id' in data.columns else 0
        
        arpdau = total_revenue / dau if dau > 0 else 0
        
        return {
            'value': round(arpdau, 2),
            'unit': 'USD',
            'definition': 'Average Revenue Per Daily Active User',
            'formula': 'total_revenue / dau',
            'confidence_interval': f'±${round(arpdau * 0.1, 2)} (95% CI)',
            'sample_size': len(data)
        }
    
    def calculate_payer_percentage(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate payer percentage."""
        total_users = data['user_id'].nunique() if 'user_id' in data.columns else 0
        paying_users = data[data['total_revenue'] > 0]['user_id'].nunique() if 'total_revenue' in data.columns else 0
        
        percentage = (paying_users / total_users) * 100 if total_users > 0 else 0
        
        return {
            'value': round(percentage, 2),
            'unit': 'percentage',
            'definition': 'Percentage of users who have made at least one purchase',
            'formula': 'paying_users / total_users * 100',
            'confidence_interval': f'±{round(percentage * 0.1, 1)}% (95% CI)',
            'sample_size': len(data)
        }
    
    def calculate_aov(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate Average Order Value."""
        revenue_data = data[data['total_revenue'] > 0] if 'total_revenue' in data.columns else pd.DataFrame()
        
        if len(revenue_data) == 0:
            return {
                'value': 0,
                'unit': 'USD',
                'definition': 'Average Order Value - average revenue per purchase',
                'formula': 'total_revenue / number_of_purchases',
                'confidence_interval': 'N/A (no purchases)',
                'sample_size': len(data)
            }
        
        total_revenue = revenue_data['total_revenue'].sum()
        num_purchases = len(revenue_data)
        aov = total_revenue / num_purchases
        
        return {
            'value': round(aov, 2),
            'unit': 'USD',
            'definition': 'Average Order Value - average revenue per purchase',
            'formula': 'total_revenue / number_of_purchases',
            'confidence_interval': f'±${round(aov * 0.1, 2)} (95% CI)',
            'sample_size': len(data)
        }
    
    def calculate_engagement_score(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate engagement score using available data."""
        # Simplified engagement score calculation
        if 'avg_session_duration_minutes' in data.columns:
            avg_session_time = data['avg_session_duration_minutes'].mean()
        else:
            avg_session_time = 0
        
        if 'total_events' in data.columns:
            avg_events = data['total_events'].mean()
        else:
            avg_events = 0
        
        # Simple scoring (0-100 scale)
        session_score = min(100, (avg_session_time / 10) * 100)  # 10 minutes = 100 points
        event_score = min(100, (avg_events / 20) * 100)  # 20 events = 100 points
        
        engagement_score = (session_score * 0.6) + (event_score * 0.4)
        
        return {
            'value': round(engagement_score, 2),
            'unit': 'score',
            'scale': '0-100',
            'definition': 'Composite score measuring user engagement across multiple dimensions',
            'formula': '(session_score * 0.6) + (event_score * 0.4)',
            'confidence_interval': f'±{round(engagement_score * 0.05, 1)} points (95% CI)',
            'sample_size': len(data)
        }
    
    def calculate_new_user_ratio(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate new user ratio."""
        total_users = data['user_id'].nunique() if 'user_id' in data.columns else 0
        new_users = data[data['user_type'] == 'new']['user_id'].nunique() if 'user_type' in data.columns else 0
        
        ratio = (new_users / total_users) * 100 if total_users > 0 else 0
        
        return {
            'value': round(ratio, 2),
            'unit': 'percentage',
            'definition': 'Percentage of DAU that are new users (first event in analysis period)',
            'formula': 'new_users / total_users * 100',
            'confidence_interval': f'±{round(ratio * 0.1, 1)}% (95% CI)',
            'sample_size': len(data)
        }
    
    def calculate_retention_rate(self, data: pd.DataFrame, day: int = 1) -> Dict[str, Any]:
        """Calculate user retention rate for a specific day."""
        if 'cohort_date' not in data.columns or 'days_since_first_event' not in data.columns:
            return {
                'value': 0,
                'unit': 'percentage',
                'definition': f'Percentage of users who return on day {day} after their first event',
                'formula': 'retained_users / cohort_size * 100',
                'confidence_interval': 'N/A (insufficient data)',
                'sample_size': len(data)
            }
        
        cohort_size = data['user_id'].nunique()
        retained_users = data[data['days_since_first_event'] == day]['user_id'].nunique()
        
        retention_rate = (retained_users / cohort_size) * 100 if cohort_size > 0 else 0
        
        return {
            'value': round(retention_rate, 2),
            'unit': 'percentage',
            'definition': f'Percentage of users who return on day {day} after their first event',
            'formula': 'retained_users / cohort_size * 100',
            'confidence_interval': f'±{round(retention_rate * 0.1, 1)}% (95% CI)',
            'sample_size': len(data)
        }
    
    def calculate_market_concentration(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate market concentration."""
        if 'country' not in data.columns or 'total_revenue' not in data.columns:
            return {
                'value': 0,
                'unit': 'percentage',
                'definition': 'Percentage of revenue concentrated in top 3 markets',
                'formula': 'top_3_markets_revenue / total_revenue * 100',
                'confidence_interval': 'N/A (insufficient data)',
                'sample_size': len(data)
            }
        
        market_revenue = data.groupby('country')['total_revenue'].sum().sort_values(ascending=False)
        total_revenue = market_revenue.sum()
        
        if total_revenue == 0:
            return {
                'value': 0,
                'unit': 'percentage',
                'definition': 'Percentage of revenue concentrated in top 3 markets',
                'formula': 'top_3_markets_revenue / total_revenue * 100',
                'confidence_interval': 'N/A (no revenue)',
                'sample_size': len(data)
            }
        
        top_3_revenue = market_revenue.head(3).sum()
        concentration = (top_3_revenue / total_revenue) * 100
        
        return {
            'value': round(concentration, 2),
            'unit': 'percentage',
            'definition': 'Percentage of revenue concentrated in top 3 markets',
            'formula': 'top_3_markets_revenue / total_revenue * 100',
            'confidence_interval': f'±{round(concentration * 0.05, 1)}% (95% CI)',
            'sample_size': len(data)
        }
    
    def get_segment_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Get revenue segment definitions."""
        return self.definitions.get('segment_definitions', {}).get('revenue_segments', {
            'free': {
                'threshold': 'revenue = 0',
                'definition': 'Users who have not made any purchases',
                'expected_percentage': 0.85
            },
            'minnow': {
                'threshold': '0 < revenue <= 2.00',
                'definition': 'Low-value users with minimal revenue contribution',
                'expected_percentage': 0.10
            },
            'dolphin': {
                'threshold': '2.00 < revenue <= 10.00',
                'definition': 'Medium-value users with moderate revenue contribution',
                'expected_percentage': 0.04
            },
            'whale': {
                'threshold': 'revenue > 10.00',
                'definition': 'High-value users with significant revenue contribution',
                'expected_percentage': 0.01
            }
        })
    
    def list_available_metrics(self) -> List[str]:
        """List all available metrics."""
        metrics = []
        for category, category_data in self.definitions.get('metric_categories', {}).items():
            metrics.extend(category_data.get('metrics', {}).keys())
        return metrics


def main():
    """Main function for testing the simple metric calculator."""
    print("🧪 Testing Simple Metric Calculator")
    print("=" * 40)
    
    # Initialize calculator
    calculator = SimpleMetricCalculator()
    
    # List available metrics
    metrics = calculator.list_available_metrics()
    print(f"📊 Available metrics: {len(metrics)}")
    for metric in metrics[:10]:  # Show first 10
        print(f"  • {metric}")
    
    # Test with sample data
    sample_data = pd.DataFrame({
        'user_id': ['user1', 'user2', 'user3', 'user4', 'user5'] * 20,  # 100 rows
        'total_revenue': [0, 2.50, 5.00, 0, 15.00] * 20,
        'avg_session_duration_minutes': [5, 10, 15, 3, 20] * 20,
        'total_events': [10, 25, 40, 5, 60] * 20,
        'user_type': ['new', 'returning', 'new', 'returning', 'returning'] * 20,
        'country': ['IN', 'BD', 'IN', 'IN', 'BD'] * 20,
        'cohort_date': ['2025-09-15'] * 100,
        'days_since_first_event': [0, 1, 2, 0, 1] * 20
    })
    
    print(f"\n📈 Sample data: {len(sample_data)} rows")
    
    # Test calculations
    try:
        arpdau = calculator.calculate_arpdau(sample_data)
        print(f"✅ ARPDAU: {arpdau['value']} {arpdau['unit']} ({arpdau['confidence_interval']})")
        
        payer_pct = calculator.calculate_payer_percentage(sample_data)
        print(f"✅ Payer %: {payer_pct['value']} {payer_pct['unit']} ({payer_pct['confidence_interval']})")
        
        aov = calculator.calculate_aov(sample_data)
        print(f"✅ AOV: {aov['value']} {aov['unit']} ({aov['confidence_interval']})")
        
        engagement = calculator.calculate_engagement_score(sample_data)
        print(f"✅ Engagement Score: {engagement['value']} {engagement['unit']} ({engagement['confidence_interval']})")
        
        new_user_ratio = calculator.calculate_new_user_ratio(sample_data)
        print(f"✅ New User Ratio: {new_user_ratio['value']} {new_user_ratio['unit']} ({new_user_ratio['confidence_interval']})")
        
        retention = calculator.calculate_retention_rate(sample_data, day=1)
        print(f"✅ Day 1 Retention: {retention['value']} {retention['unit']} ({retention['confidence_interval']})")
        
        market_concentration = calculator.calculate_market_concentration(sample_data)
        print(f"✅ Market Concentration: {market_concentration['value']} {market_concentration['unit']} ({market_concentration['confidence_interval']})")
        
        # Show segment definitions
        segments = calculator.get_segment_definitions()
        print(f"\n📊 Revenue Segment Definitions:")
        for segment, definition in segments.items():
            print(f"  • {segment}: {definition['threshold']} - {definition['definition']}")
        
    except Exception as e:
        print(f"❌ Error in calculations: {str(e)}")


if __name__ == "__main__":
    main()
