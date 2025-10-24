#!/usr/bin/env python3
"""
Daily Metrics Data Loader
Version: 2.0.0
Last Updated: 2025-10-23

Data loader for daily metrics analysis.
Loads and preprocesses daily metrics data for analysis.
"""

import sys
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class DailyMetricsDataLoader(BaseDataLoader):
    """Data loader for daily metrics analysis."""
    
    def load_data(self) -> Dict[str, Any]:
        """Load daily metrics data."""
        data = {}
        
        # Load daily metrics CSV
        daily_metrics_path = self.get_file_path("outputs/segments/daily/dau_by_date.csv")
        daily_metrics = self.load_file(daily_metrics_path, 'csv')
        
        if daily_metrics is not None:
            data['daily_metrics'] = daily_metrics
            
            # Add summary statistics
            data['summary'] = {
                'total_days': len(daily_metrics),
                'date_range': {
                    'start': daily_metrics['date'].min() if 'date' in daily_metrics.columns else None,
                    'end': daily_metrics['date'].max() if 'date' in daily_metrics.columns else None
                },
                'metrics_available': [col for col in daily_metrics.columns if col != 'date']
            }
        else:
            print("⚠️ Daily metrics data not found", file=sys.stderr)
            data['daily_metrics'] = None
            data['summary'] = {'error': 'Daily metrics data not found'}
        
        # Load any additional daily metrics files
        daily_dir = self.get_file_path("outputs/segments/daily")
        if daily_dir.exists():
            for file_path in daily_dir.glob("*.csv"):
                if file_path.name != "dau_by_date.csv":
                    file_data = self.load_file(file_path, 'csv')
                    if file_data is not None:
                        data[f"daily_{file_path.stem}"] = file_data
        
        self.data = data
        return data
    
    def get_analysis_context(self) -> Dict[str, Any]:
        """Get context for daily metrics analysis."""
        if not self.data:
            self.load_data()
        
        context = {
            'data_available': bool(self.data.get('daily_metrics') is not None),
            'metrics_count': len(self.data.get('summary', {}).get('metrics_available', [])),
            'date_range': self.data.get('summary', {}).get('date_range', {}),
            'total_days': self.data.get('summary', {}).get('total_days', 0)
        }
        
        return context
