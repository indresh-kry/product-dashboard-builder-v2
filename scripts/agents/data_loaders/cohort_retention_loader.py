#!/usr/bin/env python3
"""
Cohort Retention Data Loader
Version: 2.2.0
Last Updated: 2025-10-31

Data loader for cohort retention analysis.
Loads all retention-related cohort files and provides full dataset for LLM analysis.
Implements data size limits to prevent LLM context overflow (max ~100k tokens).
"""

import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class CohortRetentionDataLoader(BaseDataLoader):
    """Data loader for cohort retention analysis."""
    
    MAX_ROWS_FOR_LLM = 500  # Maximum rows per file to send to LLM (est. 200 tokens/row = 100k tokens)
    
    def _limit_dataframe(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """Limit dataframe size for LLM, preserving trends for time-series data."""
        if df is None or len(df) <= self.MAX_ROWS_FOR_LLM:
            return df
        
        original_count = len(df)
        half_limit = self.MAX_ROWS_FOR_LLM // 2
        
        # Check if this is time-series data (has date or cohort_date column)
        if 'cohort_date' in df.columns:
            df = df.sort_values('cohort_date').reset_index(drop=True)
            first_half = df.head(half_limit)
            last_half = df.tail(half_limit)
            df = pd.concat([first_half, last_half]).drop_duplicates().sort_values('cohort_date').reset_index(drop=True)
        elif 'date' in df.columns:
            df = df.sort_values('date').reset_index(drop=True)
            first_half = df.head(half_limit)
            last_half = df.tail(half_limit)
            df = pd.concat([first_half, last_half]).drop_duplicates().sort_values('date').reset_index(drop=True)
        else:
            # No date column, just take first N rows
            df = df.head(self.MAX_ROWS_FOR_LLM)
        
        print(f"📊 {file_name} summarized from {original_count} to {len(df)} rows")
        return df
    
    def load_data(self) -> Dict[str, Any]:
        """Load cohort retention data from all available files."""
        data = {}
        
        # Load all cohort retention-related files
        cohort_dir = self.get_file_path("outputs/segments/cohort")
        
        # Primary retention file
        retention_path = cohort_dir / "retention_by_cohort_date.csv"
        retention_data = self.load_file(retention_path, 'csv')
        original_retention_count = len(retention_data) if retention_data is not None else 0
        if retention_data is not None:
            retention_data = self._limit_dataframe(retention_data, "retention_by_cohort_date.csv")
            data['retention_by_cohort'] = retention_data
        else:
            data['retention_by_cohort'] = None
        
        # Revenue by cohort
        revenue_path = cohort_dir / "revenue_by_cohort_date.csv"
        revenue_data = self.load_file(revenue_path, 'csv')
        if revenue_data is not None:
            revenue_data = self._limit_dataframe(revenue_data, "revenue_by_cohort_date.csv")
            data['revenue_by_cohort'] = revenue_data
        
        # DAU by cohort
        dau_path = cohort_dir / "dau_by_cohort_date.csv"
        dau_data = self.load_file(dau_path, 'csv')
        if dau_data is not None:
            dau_data = self._limit_dataframe(dau_data, "dau_by_cohort_date.csv")
            data['dau_by_cohort'] = dau_data
        
        # Engagement by cohort
        engagement_path = cohort_dir / "engagement_by_cohort_date.csv"
        engagement_data = self.load_file(engagement_path, 'csv')
        if engagement_data is not None:
            engagement_data = self._limit_dataframe(engagement_data, "engagement_by_cohort_date.csv")
            data['engagement_by_cohort'] = engagement_data
        
        # Funnel by cohort
        funnel_path = cohort_dir / "funnel_by_cohort_date.csv"
        funnel_data = self.load_file(funnel_path, 'csv')
        if funnel_data is not None:
            funnel_data = self._limit_dataframe(funnel_data, "funnel_by_cohort_date.csv")
            data['funnel_by_cohort'] = funnel_data
        
        # Create summary
        if retention_data is not None:
            data['summary'] = {
                'total_cohorts': len(retention_data),
                'original_rows': original_retention_count if original_retention_count > self.MAX_ROWS_FOR_LLM else None,
                'retention_columns': list(retention_data.columns) if hasattr(retention_data, 'columns') else [],
                'files_loaded': len([k for k in data.keys() if k != 'summary' and data[k] is not None]),
                'note': f"Primary dataset summarized from {original_retention_count} to {len(retention_data)} rows for LLM analysis" if original_retention_count > self.MAX_ROWS_FOR_LLM else None
            }
        else:
            data['summary'] = {
                'error': 'Primary retention data not found',
                'files_loaded': len([k for k in data.keys() if k != 'summary' and data[k] is not None])
            }
        
        self.data = data
        return data
