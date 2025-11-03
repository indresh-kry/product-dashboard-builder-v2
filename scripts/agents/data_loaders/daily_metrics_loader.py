#!/usr/bin/env python3
"""
Daily Metrics Data Loader
Version: 2.1.0
Last Updated: 2025-10-31

Data loader for daily metrics analysis.
Loads and preprocesses daily metrics data for analysis.
Implements data size limits to prevent LLM context overflow (max ~100k tokens).
"""

import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class DailyMetricsDataLoader(BaseDataLoader):
    """Data loader for daily metrics analysis."""
    
    MAX_ROWS_FOR_LLM = 500  # Maximum rows to send to LLM (est. 200 tokens/row = 100k tokens)
    
    def load_data(self) -> Dict[str, Any]:
        """Load daily metrics data with size limits to prevent LLM overflow."""
        data = {}
        
        # Load daily metrics CSV
        daily_metrics_path = self.get_file_path("outputs/segments/daily/dau_by_date.csv")
        daily_metrics = self.load_file(daily_metrics_path, 'csv')
        
        if daily_metrics is not None:
            # Store original row count before summarizing
            original_row_count = len(daily_metrics)
            
            # If dataset is too large, summarize it
            if original_row_count > self.MAX_ROWS_FOR_LLM:
                print(f"📊 Daily metrics has {original_row_count} rows, summarizing to {self.MAX_ROWS_FOR_LLM} for LLM")
                half_limit = self.MAX_ROWS_FOR_LLM // 2
                if 'date' in daily_metrics.columns:
                    daily_metrics = daily_metrics.sort_values('date').reset_index(drop=True)
                    # First half
                    first_half = daily_metrics.head(half_limit)
                    # Last half
                    last_half = daily_metrics.tail(half_limit)
                    # Combine
                    daily_metrics_summarized = pd.concat([first_half, last_half]).drop_duplicates().sort_values('date').reset_index(drop=True)
                    daily_metrics = daily_metrics_summarized
                else:
                    # If no date column, just take first N rows
                    daily_metrics = daily_metrics.head(self.MAX_ROWS_FOR_LLM)
                print(f"✅ Summarized from {original_row_count} to {len(daily_metrics)} rows")
            
            # Add summary statistics
            data['summary'] = {
                'total_days': len(daily_metrics),
                'original_rows': original_row_count if original_row_count > self.MAX_ROWS_FOR_LLM else None,
                'date_range': {
                    'start': daily_metrics['date'].min() if 'date' in daily_metrics.columns else None,
                    'end': daily_metrics['date'].max() if 'date' in daily_metrics.columns else None
                },
                'metrics_available': [col for col in daily_metrics.columns if col != 'date'],
                'note': f"Dataset summarized from {original_row_count} to {len(daily_metrics)} rows for LLM analysis" if original_row_count > self.MAX_ROWS_FOR_LLM else None
            }
            
            data['daily_metrics'] = daily_metrics
        else:
            print("⚠️ Daily metrics data not found", file=sys.stderr)
            data['daily_metrics'] = None
            data['summary'] = {'error': 'Daily metrics data not found'}
        
        # Load any additional daily metrics files (with size limits)
        daily_dir = self.get_file_path("outputs/segments/daily")
        if daily_dir.exists():
            for file_path in daily_dir.glob("*.csv"):
                if file_path.name != "dau_by_date.csv":
                    file_data = self.load_file(file_path, 'csv')
                    if file_data is not None:
                        # Apply same size limit to other daily files
                        if len(file_data) > self.MAX_ROWS_FOR_LLM:
                            file_data = file_data.head(self.MAX_ROWS_FOR_LLM)
                            print(f"📊 {file_path.name} limited to {self.MAX_ROWS_FOR_LLM} rows")
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
