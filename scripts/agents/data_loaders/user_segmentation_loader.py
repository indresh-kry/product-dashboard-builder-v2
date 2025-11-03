#!/usr/bin/env python3
"""
User Segmentation Data Loader
Version: 2.1.0
Last Updated: 2025-10-31

Data loader for user segmentation analysis.
Implements data size limits to prevent LLM context overflow (max ~100k tokens).
"""

import sys
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class UserSegmentationDataLoader(BaseDataLoader):
    """Data loader for user segmentation analysis."""
    
    MAX_JSON_SIZE_KB = 100  # Maximum JSON size in KB to send to LLM (est. 1KB = 250 tokens, 100KB = 25k tokens)
    
    def load_data(self) -> Dict[str, Any]:
        """Load user segmentation data with size limits to prevent LLM overflow."""
        data = {}
        
        # Load user segments JSON
        segments_path = self.get_file_path("outputs/segments/segment_analysis_report.json")
        segments = self.load_file(segments_path, 'json')
        
        if segments is not None:
            # For JSON data, check size and potentially limit if it's too large
            import os
            file_size_kb = os.path.getsize(segments_path) / 1024 if segments_path.exists() else 0
            
            if file_size_kb > self.MAX_JSON_SIZE_KB:
                print(f"📊 User segments JSON is {file_size_kb:.1f} KB, larger than {self.MAX_JSON_SIZE_KB} KB limit")
                # For JSON, we'll keep it but add a note
                data['user_segments'] = segments
                data['summary'] = {
                    'total_segments': len(segments) if isinstance(segments, dict) else 0,
                    'segment_types': list(segments.keys()) if isinstance(segments, dict) else [],
                    'file_size_kb': round(file_size_kb, 1),
                    'note': f"Large JSON file ({file_size_kb:.1f} KB) - consider summarizing in future"
                }
            else:
                data['user_segments'] = segments
                data['summary'] = {
                    'total_segments': len(segments) if isinstance(segments, dict) else 0,
                    'segment_types': list(segments.keys()) if isinstance(segments, dict) else []
                }
            
            # Also try to load CSV files if they exist (user-level segments)
            user_level_dir = self.get_file_path("outputs/segments/user_level")
            if user_level_dir.exists():
                for file_path in user_level_dir.glob("*.csv"):
                    file_data = self.load_file(file_path, 'csv')
                    if file_data is not None:
                        # Limit CSV files to 500 rows
                        MAX_ROWS = 500
                        if len(file_data) > MAX_ROWS:
                            print(f"📊 {file_path.name} has {len(file_data)} rows, limiting to {MAX_ROWS} for LLM")
                            file_data = file_data.head(MAX_ROWS)
                        data[f"user_level_{file_path.stem}"] = file_data
        else:
            data['user_segments'] = None
            data['summary'] = {'error': 'User segments data not found'}
        
        self.data = data
        return data
