#!/usr/bin/env python3
"""
User Segmentation Data Loader
Version: 2.0.0
Last Updated: 2025-10-23

Data loader for user segmentation analysis.
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class UserSegmentationDataLoader(BaseDataLoader):
    """Data loader for user segmentation analysis."""
    
    def load_data(self) -> Dict[str, Any]:
        """Load user segmentation data."""
        data = {}
        
        # Load user segments CSV
        segments_path = self.get_file_path("outputs/segments/segment_analysis_report.json")
        segments = self.load_file(segments_path, 'json')
        
        if segments is not None:
            data['user_segments'] = segments
            data['summary'] = {
                'total_segments': len(segments),
                'segment_types': list(segments.columns) if hasattr(segments, 'columns') else []
            }
        else:
            data['user_segments'] = None
            data['summary'] = {'error': 'User segments data not found'}
        
        self.data = data
        return data
