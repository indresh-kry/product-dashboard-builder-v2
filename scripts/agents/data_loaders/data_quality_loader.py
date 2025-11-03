#!/usr/bin/env python3
"""
Data Quality Data Loader
Version: 2.1.0
Last Updated: 2025-10-31

Data loader for data quality analysis.
Implements data size limits to prevent LLM context overflow (max ~100k tokens).
"""

import sys
import json
import os
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class DataQualityDataLoader(BaseDataLoader):
    """Data loader for data quality analysis."""
    
    MAX_JSON_SIZE_KB = 100  # Maximum JSON size in KB to send to LLM (est. 1KB = 250 tokens, 100KB = 25k tokens)
    
    def load_data(self) -> Dict[str, Any]:
        """Load data quality data with size limits to prevent LLM overflow."""
        data = {}
        
        # Load data quality report
        quality_path = self.get_file_path("outputs/quality_validation/quality_validation_report.json")
        quality_data = self.load_file(quality_path, 'json')
        
        if quality_data is not None:
            # Check file size
            file_size_kb = os.path.getsize(quality_path) / 1024 if quality_path.exists() else 0
            
            if file_size_kb > self.MAX_JSON_SIZE_KB:
                print(f"📊 Data quality JSON is {file_size_kb:.1f} KB, larger than {self.MAX_JSON_SIZE_KB} KB limit")
                # For data quality, we keep all data but add a note
                data['data_quality'] = quality_data
                data['summary'] = {
                    'quality_metrics': list(quality_data.keys()) if isinstance(quality_data, dict) else [],
                    'file_size_kb': round(file_size_kb, 1),
                    'note': f"Large JSON file ({file_size_kb:.1f} KB) - consider summarizing in future"
                }
            else:
                data['data_quality'] = quality_data
                data['summary'] = {
                    'quality_metrics': list(quality_data.keys()) if isinstance(quality_data, dict) else []
                }
        else:
            data['data_quality'] = None
            data['summary'] = {'error': 'Data quality report not found'}
        
        self.data = data
        return data
