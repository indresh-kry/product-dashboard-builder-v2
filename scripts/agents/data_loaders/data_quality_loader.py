#!/usr/bin/env python3
"""
Data Quality Data Loader
Version: 2.0.0
Last Updated: 2025-10-23

Data loader for data quality analysis.
"""

import sys
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class DataQualityDataLoader(BaseDataLoader):
    """Data loader for data quality analysis."""
    
    def load_data(self) -> Dict[str, Any]:
        """Load data quality data."""
        data = {}
        
        # Load data quality report
        quality_path = self.get_file_path("outputs/insights/data_quality_report.json")
        quality_data = self.load_file(quality_path, 'json')
        
        if quality_data is not None:
            data['data_quality'] = quality_data
            data['summary'] = {
                'quality_metrics': list(quality_data.keys()) if isinstance(quality_data, dict) else []
            }
        else:
            data['data_quality'] = None
            data['summary'] = {'error': 'Data quality report not found'}
        
        self.data = data
        return data
