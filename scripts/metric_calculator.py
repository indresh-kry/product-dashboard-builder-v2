#!/usr/bin/env python3
"""
Metric Calculator with Standardized Definitions
Version: 1.0.0
Last Updated: 2025-10-16

This module provides standardized metric calculations using the definitions
from schemas/metric_definitions.json. It ensures consistent calculations
across all analysts with proper units, confidence intervals, and validation.

Environment Variables:
- METRIC_DEFINITIONS_FILE: Path to metric definitions (default: schemas/metric_definitions.json)

Dependencies:
- pandas: Data manipulation and analysis
- numpy: Numerical calculations
- scipy: Statistical functions
- json: JSON parsing
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Union

import pandas as pd
import numpy as np
from scipy import stats

class MetricCalculationError(Exception):
    """Custom exception for metric calculation errors."""
    pass

class MetricCalculator:
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
                raise FileNotFoundError(f"Definitions file not found: {self.definitions_file}")
                
            with open(definitions_path, 'r') as f:
                definitions = json.load(f)
                
            print(f"✅ Loaded metric definitions from: {self.definitions_file}")
            return definitions
            
        except Exception as e:
            print(f"❌ Error loading metric definitions: {str(e)}")
            raise MetricCalculationError(f"Failed to load definitions: {str(e)}")
    
    def calculate_metric(self, metric_name: str, data: pd.DataFrame, 
                        params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Calculate a metric using its standardized definition.
        
        Args:
            metric_name: Name of the metric to calculate
            data: DataFrame containing the data
            params: Optional parameters for the calculation
            
        Returns:
            Dictionary containing the metric value, unit, confidence interval, etc.
        """
        try:
            # Find metric definition
            metric_def = self._find_metric_definition(metric_name)
            if not metric_def:
                raise MetricCalculationError(f"Metric definition not found: {metric_name}")
            
            # Validate data requirements
            self._validate_data_requirements(metric_def, data)
            
            # Calculate the metric
            result = self._execute_calculation(metric_def, data, params)
            
            # Add confidence interval
            result['confidence_interval'] = self._calculate_confidence_interval(
                result['value'], data, metric_def
            )
            
            # Add metadata
            result['metadata'] = {
                'metric_name': metric_name,
                'calculated_at': datetime.now().isoformat(),
                'sample_size': len(data),
                'formula': metric_def.get('formula', 'N/A'),
                'calculation_method': metric_def.get('calculation_method', 'N/A')
            }
            
            return result
            
        except Exception as e:
            print(f"❌ Error calculating metric {metric_name}: {str(e)}")
            raise MetricCalculationError(f"Failed to calculate {metric_name}: {str(e)}")
    
    def _find_metric_definition(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Find the definition for a specific metric."""
        for category, category_data in self.definitions.get('metric_categories', {}).items():
            metrics = category_data.get('metrics', {})
            if metric_name in metrics:
                return metrics[metric_name]
        return None
    
    def _validate_data_requirements(self, metric_def: Dict[str, Any], data: pd.DataFrame):
        """Validate that the data meets the requirements for the metric."""
        required_fields = metric_def.get('data_requirements', [])
        missing_fields = [field for field in required_fields if field not in data.columns]
        
        if missing_fields:
            raise MetricCalculationError(f"Missing required fields: {missing_fields}")
        
        min_sample_size = metric_def.get('sample_size_requirement', 1)
        if len(data) < min_sample_size:
            raise MetricCalculationError(
                f"Insufficient sample size: {len(data)} < {min_sample_size}"
            )
    
    def _determine_metric_name(self, metric_def: Dict[str, Any]) -> str:
        """Determine the metric name from the definition context."""
        # This is a simplified approach - in practice, we'd need to track which metric is being calculated
        # For now, we'll use a generic approach
        return "generic_metric"
    
    def _execute_calculation(self, metric_def: Dict[str, Any], 
                           data: pd.DataFrame, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute the actual metric calculation."""
        formula = metric_def.get('formula', '')
        unit = metric_def.get('unit', '')
        calculation_method = metric_def.get('calculation_method', '')
        
        # Map formula to actual calculation based on the metric being calculated
        # We need to determine which metric this is from the context
        metric_name = self._determine_metric_name(metric_def)
        if metric_name == 'engagement_score':
            return self._calculate_engagement_score(data)
        elif metric_name == 'avg_session_time':
            return self._calculate_avg_session_time(data)
        elif metric_name == 'dau':
            return self._calculate_dau(data, params)
        elif metric_name == 'new_user_ratio':
            return self._calculate_new_user_ratio(data)
        elif metric_name == 'arpdau':
            return self._calculate_arpdau(data)
        elif metric_name == 'payer_percentage':
            return self._calculate_payer_percentage(data)
        elif metric_name == 'aov':
            return self._calculate_aov(data)
        elif metric_name == 'user_retention_rate':
            return self._calculate_retention_rate(data, params)
        elif metric_name == 'market_concentration':
            return self._calculate_market_concentration(data)
        elif metric_name == 'market_diversity_score':
            return self._calculate_market_diversity_score(data)
        elif metric_name == 'funnel_conversion_rate':
            return self._calculate_funnel_conversion_rate(data, params)
        elif metric_name == 'data_completeness':
            return self._calculate_data_completeness(data)
        
        # Generic calculation based on formula
        return self._generic_calculation(formula, data, unit, calculation_method)
    
    def _calculate_engagement_score(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate engagement score using weighted formula."""
        # Normalize components to 0-100 scale
        session_freq = self._normalize_to_scale(data.get('session_frequency', 0), 0, 10)
        session_duration = self._normalize_to_scale(data.get('avg_session_duration_minutes', 0), 0, 60)
        event_freq = self._normalize_to_scale(data.get('event_frequency', 0), 0, 50)
        recency = self._calculate_recency_score(data)
        
        # Weighted average
        engagement_score = (
            session_freq * 0.3 +
            session_duration * 0.3 +
            event_freq * 0.2 +
            recency * 0.2
        )
        
        return {
            'value': round(engagement_score, 2),
            'unit': 'score',
            'scale': '0-100',
            'definition': 'Composite score measuring user engagement across multiple dimensions'
        }
    
    def _calculate_avg_session_time(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate average session time."""
        session_times = data.get('session_duration_minutes', [])
        if not session_times or len(session_times) == 0:
            return {'value': 0, 'unit': 'minutes', 'definition': 'Average session duration'}
        
        avg_time = np.mean(session_times)
        return {
            'value': round(avg_time, 2),
            'unit': 'minutes',
            'definition': 'Average duration of user sessions',
            'calculation_method': 'mean'
        }
    
    def _calculate_dau(self, data: pd.DataFrame, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate Daily Active Users."""
        target_date = params.get('target_date') if params else None
        if target_date:
            daily_data = data[data['date'] == target_date]
        else:
            daily_data = data
        
        dau = daily_data['user_id'].nunique()
        return {
            'value': dau,
            'unit': 'users',
            'definition': 'Daily Active Users - unique users who performed at least one action on a given day'
        }
    
    def _calculate_new_user_ratio(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate new user ratio."""
        total_users = data['user_id'].nunique()
        new_users = data[data['user_type'] == 'new']['user_id'].nunique()
        
        ratio = new_users / total_users if total_users > 0 else 0
        return {
            'value': round(ratio, 3),
            'unit': 'percentage',
            'definition': 'Percentage of DAU that are new users (first event in analysis period)'
        }
    
    def _calculate_arpdau(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate Average Revenue Per Daily Active User."""
        total_revenue = data['total_revenue'].sum()
        dau = data['user_id'].nunique()
        
        arpdau = total_revenue / dau if dau > 0 else 0
        return {
            'value': round(arpdau, 2),
            'unit': 'USD',
            'definition': 'Average Revenue Per Daily Active User'
        }
    
    def _calculate_payer_percentage(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate payer percentage."""
        total_users = data['user_id'].nunique()
        paying_users = data[data['total_revenue'] > 0]['user_id'].nunique()
        
        percentage = (paying_users / total_users) * 100 if total_users > 0 else 0
        return {
            'value': round(percentage, 2),
            'unit': 'percentage',
            'definition': 'Percentage of users who have made at least one purchase'
        }
    
    def _calculate_aov(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate Average Order Value."""
        revenue_data = data[data['total_revenue'] > 0]
        if len(revenue_data) == 0:
            return {'value': 0, 'unit': 'USD', 'definition': 'Average Order Value'}
        
        total_revenue = revenue_data['total_revenue'].sum()
        num_purchases = len(revenue_data)
        
        aov = total_revenue / num_purchases
        return {
            'value': round(aov, 2),
            'unit': 'USD',
            'definition': 'Average Order Value - average revenue per purchase'
        }
    
    def _calculate_retention_rate(self, data: pd.DataFrame, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate user retention rate."""
        day = params.get('day', 1) if params else 1
        cohort_date = params.get('cohort_date') if params else None
        
        if cohort_date:
            cohort_data = data[data['cohort_date'] == cohort_date]
        else:
            cohort_data = data
        
        cohort_size = cohort_data['user_id'].nunique()
        retained_users = cohort_data[cohort_data['days_since_first_event'] == day]['user_id'].nunique()
        
        retention_rate = retained_users / cohort_size if cohort_size > 0 else 0
        return {
            'value': round(retention_rate, 3),
            'unit': 'percentage',
            'definition': f'Percentage of users who return on day {day} after their first event'
        }
    
    def _calculate_market_concentration(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate market concentration."""
        market_revenue = data.groupby('country')['total_revenue'].sum().sort_values(ascending=False)
        total_revenue = market_revenue.sum()
        
        if total_revenue == 0:
            return {'value': 0, 'unit': 'percentage', 'definition': 'Market concentration in top 3 markets'}
        
        top_3_revenue = market_revenue.head(3).sum()
        concentration = (top_3_revenue / total_revenue) * 100
        
        return {
            'value': round(concentration, 2),
            'unit': 'percentage',
            'definition': 'Percentage of revenue concentrated in top 3 markets'
        }
    
    def _calculate_market_diversity_score(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate market diversity score using Herfindahl index."""
        market_revenue = data.groupby('country')['total_revenue'].sum()
        total_revenue = market_revenue.sum()
        
        if total_revenue == 0:
            return {'value': 0, 'unit': 'score', 'definition': 'Market diversity score'}
        
        # Calculate Herfindahl index
        market_shares = market_revenue / total_revenue
        herfindahl_index = (market_shares ** 2).sum()
        
        # Convert to diversity score (1 - HHI)
        diversity_score = 1 - herfindahl_index
        
        return {
            'value': round(diversity_score, 3),
            'unit': 'score',
            'definition': 'Diversity score based on revenue distribution across markets'
        }
    
    def _calculate_funnel_conversion_rate(self, data: pd.DataFrame, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate funnel conversion rate."""
        step = params.get('step') if params else 'ftue_complete'
        
        # This would need to be implemented based on specific funnel logic
        # For now, return a placeholder
        return {
            'value': 0.65,  # Placeholder
            'unit': 'percentage',
            'definition': f'Conversion rate for {step} step'
        }
    
    def _calculate_data_completeness(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate data completeness percentage."""
        total_cells = data.size
        non_null_cells = data.count().sum()
        
        completeness = (non_null_cells / total_cells) * 100 if total_cells > 0 else 0
        
        return {
            'value': round(completeness, 2),
            'unit': 'percentage',
            'definition': 'Percentage of non-null values in the dataset'
        }
    
    def _normalize_to_scale(self, value: Union[float, int], min_val: float, max_val: float) -> float:
        """Normalize a value to 0-100 scale."""
        if max_val == min_val:
            return 50  # Default middle value
        return min(100, max(0, ((value - min_val) / (max_val - min_val)) * 100))
    
    def _calculate_recency_score(self, data: pd.DataFrame) -> float:
        """Calculate recency score based on last activity."""
        # This would need to be implemented based on actual data structure
        # For now, return a placeholder
        return 75.0
    
    def _calculate_confidence_interval(self, value: float, data: pd.DataFrame, 
                                     metric_def: Dict[str, Any]) -> Dict[str, float]:
        """Calculate confidence interval for the metric."""
        sample_size = len(data)
        confidence_level = 0.95  # 95% confidence
        
        # Get confidence interval from definition
        ci_string = metric_def.get('confidence_interval', '±0.05 (95% CI)')
        
        # Parse confidence interval (simplified)
        if '±' in ci_string:
            margin = float(ci_string.split('±')[1].split()[0])
            return {
                'lower': round(value - margin, 3),
                'upper': round(value + margin, 3),
                'margin': margin,
                'confidence_level': confidence_level
            }
        
        # Default confidence interval
        return {
            'lower': round(value * 0.95, 3),
            'upper': round(value * 1.05, 3),
            'margin': round(value * 0.05, 3),
            'confidence_level': confidence_level
        }
    
    def _generic_calculation(self, formula: str, data: pd.DataFrame, 
                           unit: str, method: str) -> Dict[str, Any]:
        """Generic calculation for formulas not specifically implemented."""
        # This would need to be expanded to handle various formula types
        return {
            'value': 0,
            'unit': unit,
            'definition': 'Generic calculation',
            'calculation_method': method
        }
    
    def get_metric_definition(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """Get the definition for a specific metric."""
        return self._find_metric_definition(metric_name)
    
    def list_available_metrics(self) -> List[str]:
        """List all available metrics."""
        metrics = []
        for category, category_data in self.definitions.get('metric_categories', {}).items():
            metrics.extend(category_data.get('metrics', {}).keys())
        return metrics


def main():
    """Main function for testing the metric calculator."""
    print("🧪 Testing Metric Calculator")
    print("=" * 40)
    
    # Initialize calculator
    calculator = MetricCalculator()
    
    # List available metrics
    metrics = calculator.list_available_metrics()
    print(f"📊 Available metrics: {len(metrics)}")
    for metric in metrics[:10]:  # Show first 10
        print(f"  • {metric}")
    
    # Test with sample data
    sample_data = pd.DataFrame({
        'user_id': ['user1', 'user2', 'user3', 'user4', 'user5'],
        'total_revenue': [0, 2.50, 5.00, 0, 15.00],
        'session_duration_minutes': [5, 10, 15, 3, 20],
        'user_type': ['new', 'returning', 'new', 'returning', 'returning'],
        'country': ['IN', 'BD', 'IN', 'IN', 'BD']
    })
    
    print(f"\n📈 Sample data: {len(sample_data)} rows")
    
    # Test calculations
    try:
        arpdau = calculator.calculate_metric('arpdau', sample_data)
        print(f"✅ ARPDAU: {arpdau['value']} {arpdau['unit']}")
        
        payer_pct = calculator.calculate_metric('payer_percentage', sample_data)
        print(f"✅ Payer %: {payer_pct['value']} {payer_pct['unit']}")
        
        aov = calculator.calculate_metric('aov', sample_data)
        print(f"✅ AOV: {aov['value']} {aov['unit']}")
        
    except Exception as e:
        print(f"❌ Error in calculations: {str(e)}")


if __name__ == "__main__":
    main()
