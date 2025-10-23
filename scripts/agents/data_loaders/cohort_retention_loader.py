#!/usr/bin/env python3
"""
Cohort Retention Data Loader
Version: 2.0.0
Last Updated: 2025-10-23

Data loader for cohort retention analysis.
"""

import sys
from pathlib import Path
from typing import Dict, Any
from .base_loader import BaseDataLoader

class CohortRetentionDataLoader(BaseDataLoader):
    """Data loader for cohort retention analysis."""
    
    def load_data(self) -> Dict[str, Any]:
        """Load cohort retention data."""
        data = {}
        
        # Load cohort retention CSV
        cohort_path = self.get_file_path("outputs/segments/cohort/retention_by_cohort.csv")
        cohort_data = self.load_file(cohort_path, 'csv')
        
        if cohort_data is not None:
            data['cohort_retention'] = cohort_data
            data['summary'] = {
                'total_cohorts': len(cohort_data),
                'cohort_types': list(cohort_data.columns) if hasattr(cohort_data, 'columns') else []
            }
        else:
            data['cohort_retention'] = None
            data['summary'] = {'error': 'Cohort retention data not found'}
        
        self.data = data
        return data
