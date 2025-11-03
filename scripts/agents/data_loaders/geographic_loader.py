#!/usr/bin/env python3
"""
Geographic Data Loader
Version: 2.2.0
Last Updated: 2025-10-31

Data loader for geographic analysis.
Filters geographies where average daily users > 100.
Implements data size limits to prevent LLM context overflow (max ~100k tokens).
"""

import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class GeographicDataLoader(BaseDataLoader):
    """Data loader for geographic analysis."""
    
    MAX_ROWS_FOR_LLM = 500  # Maximum rows to send to LLM (est. 200 tokens/row = 100k tokens)
    
    def load_data(self) -> Dict[str, Any]:
        """Load geographic data and filter by average daily users > 100, with size limits."""
        data = {}
        
        # Load geographic data CSV
        geo_path = self.get_file_path("outputs/segments/daily/revenue_by_country.csv")
        geo_data = self.load_file(geo_path, 'csv')
        
        if geo_data is not None:
            # Filter geographies where average daily users > 100
            geo_data_filtered = self._filter_by_avg_daily_users(geo_data, threshold=100)
            
            # Store original row count before limiting
            original_row_count = len(geo_data_filtered)
            
            # Apply row limit if needed
            if original_row_count > self.MAX_ROWS_FOR_LLM:
                print(f"📊 Geographic data has {original_row_count} rows after filtering, limiting to {self.MAX_ROWS_FOR_LLM} for LLM")
                # For geographic data, prioritize by revenue or user count if available
                if 'total_revenue' in geo_data_filtered.columns:
                    geo_data_filtered = geo_data_filtered.nlargest(self.MAX_ROWS_FOR_LLM, 'total_revenue')
                elif 'revenue_users' in geo_data_filtered.columns:
                    geo_data_filtered = geo_data_filtered.nlargest(self.MAX_ROWS_FOR_LLM, 'revenue_users')
                else:
                    # No ranking column, just take first N rows
                    geo_data_filtered = geo_data_filtered.head(self.MAX_ROWS_FOR_LLM)
                print(f"✅ Limited to {len(geo_data_filtered)} rows")
            
            data['geographic_data'] = geo_data_filtered
            data['summary'] = {
                'total_locations': len(geo_data_filtered['country'].unique()) if 'country' in geo_data_filtered.columns else 0,
                'total_rows': len(geo_data_filtered),
                'original_rows': original_row_count if original_row_count > self.MAX_ROWS_FOR_LLM else None,
                'original_locations': len(geo_data['country'].unique()) if 'country' in geo_data.columns else 0,
                'filtered_out': len(geo_data) - original_row_count,
                'location_types': list(geo_data_filtered.columns) if hasattr(geo_data_filtered, 'columns') else [],
                'note': f"Dataset limited from {original_row_count} to {len(geo_data_filtered)} rows for LLM analysis" if original_row_count > self.MAX_ROWS_FOR_LLM else None
            }
            
            print(f"🌍 Geographic filtering: {data['summary']['total_locations']} countries with avg daily users > 100 "
                  f"(filtered out {data['summary']['filtered_out']} rows)")
        else:
            data['geographic_data'] = None
            data['summary'] = {'error': 'Geographic data not found'}
        
        self.data = data
        return data
    
    def _filter_by_avg_daily_users(self, df: pd.DataFrame, threshold: int = 100) -> pd.DataFrame:
        """Filter geographies where average daily users > threshold.
        
        Args:
            df: DataFrame with columns including 'country' and 'revenue_users' (or similar user count column)
            threshold: Minimum average daily users required
            
        Returns:
            Filtered DataFrame containing only countries meeting the threshold
        """
        if df is None or len(df) == 0:
            return df
        
        # Identify the user count column (could be 'revenue_users', 'total_users', 'dau', etc.)
        user_col = None
        for col in ['revenue_users', 'total_users', 'dau', 'users', 'user_count']:
            if col in df.columns:
                user_col = col
                break
        
        if user_col is None:
            print("⚠️ Warning: No user count column found, returning all data")
            return df
        
        if 'country' not in df.columns:
            print("⚠️ Warning: No 'country' column found, returning all data")
            return df
        
        # Calculate average daily users per country
        country_avg_users = df.groupby('country')[user_col].mean()
        
        # Filter countries with average > threshold
        qualifying_countries = country_avg_users[country_avg_users > threshold].index.tolist()
        
        if len(qualifying_countries) == 0:
            print(f"⚠️ Warning: No countries found with average daily users > {threshold}")
            return df.iloc[0:0].copy()  # Return empty DataFrame with same structure
        
        # Filter dataframe to only qualifying countries
        filtered_df = df[df['country'].isin(qualifying_countries)].copy()
        
        print(f"📊 Filtered to {len(qualifying_countries)} countries: {', '.join(sorted(qualifying_countries))}")
        
        return filtered_df
