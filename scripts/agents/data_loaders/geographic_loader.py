#!/usr/bin/env python3
"""
Geographic Data Loader
Version: 2.0.0
Last Updated: 2025-10-23

Data loader for geographic analysis.
"""

import sys
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class GeographicDataLoader(BaseDataLoader):
    """Data loader for geographic analysis."""
    
    def load_data(self) -> Dict[str, Any]:
        """Load geographic data."""
        data = {}
        
        # Load geographic data CSV
        geo_path = self.get_file_path("outputs/segments/daily/revenue_by_country.csv")
        geo_data = self.load_file(geo_path, 'csv')
        
        if geo_data is not None:
            data['geographic_data'] = geo_data
            data['summary'] = {
                'total_locations': len(geo_data),
                'location_types': list(geo_data.columns) if hasattr(geo_data, 'columns') else []
            }
        else:
            data['geographic_data'] = None
            data['summary'] = {'error': 'Geographic data not found'}
        
        self.data = data
        return data
