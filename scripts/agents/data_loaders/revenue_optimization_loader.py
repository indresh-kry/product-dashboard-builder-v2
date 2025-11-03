#!/usr/bin/env python3
"""
Revenue Optimization Data Loader
Version: 2.1.0
Last Updated: 2025-10-31

Data loader for revenue optimization analysis.
Implements data size limits to prevent LLM context overflow (max ~100k tokens).
"""

import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class RevenueOptimizationDataLoader(BaseDataLoader):
    """Data loader for revenue optimization analysis."""
    
    MAX_ROWS_FOR_LLM = 500  # Maximum rows to send to LLM (est. 200 tokens/row = 100k tokens)
    
    def load_data(self) -> Dict[str, Any]:
        """Load revenue optimization data with size limits to prevent LLM overflow."""
        data = {}
        
        # Load revenue data CSV
        revenue_path = self.get_file_path("outputs/segments/daily/revenue_by_date.csv")
        revenue_data = self.load_file(revenue_path, 'csv')
        
        if revenue_data is not None:
            # Store original row count before summarizing
            original_row_count = len(revenue_data)
            
            # If dataset is too large, summarize it
            if original_row_count > self.MAX_ROWS_FOR_LLM:
                print(f"📊 Revenue data has {original_row_count} rows, summarizing to {self.MAX_ROWS_FOR_LLM} for LLM")
                half_limit = self.MAX_ROWS_FOR_LLM // 2
                if 'date' in revenue_data.columns:
                    revenue_data = revenue_data.sort_values('date').reset_index(drop=True)
                    # First half
                    first_half = revenue_data.head(half_limit)
                    # Last half
                    last_half = revenue_data.tail(half_limit)
                    # Combine
                    revenue_data_summarized = pd.concat([first_half, last_half]).drop_duplicates().sort_values('date').reset_index(drop=True)
                    revenue_data = revenue_data_summarized
                else:
                    # If no date column, just take first N rows
                    revenue_data = revenue_data.head(self.MAX_ROWS_FOR_LLM)
                print(f"✅ Summarized from {original_row_count} to {len(revenue_data)} rows")
            
            data['revenue_data'] = revenue_data
            data['summary'] = {
                'total_revenue_records': len(revenue_data),
                'original_rows': original_row_count if original_row_count > self.MAX_ROWS_FOR_LLM else None,
                'revenue_metrics': list(revenue_data.columns) if hasattr(revenue_data, 'columns') else [],
                'note': f"Dataset summarized from {original_row_count} to {len(revenue_data)} rows for LLM analysis" if original_row_count > self.MAX_ROWS_FOR_LLM else None
            }
        else:
            data['revenue_data'] = None
            data['summary'] = {'error': 'Revenue data not found'}
        
        self.data = data
        return data
