#!/usr/bin/env python3
"""
Base Data Loader
Version: 2.0.0
Last Updated: 2025-10-23

Base class for all data loaders.
Provides common functionality for loading and preprocessing data.
"""

import os
import json
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pathlib import Path

class BaseDataLoader(ABC):
    """Base class for all data loaders."""
    
    def __init__(self, run_hash: str):
        self.run_hash = run_hash
        self.base_path = Path(f"run_logs/{run_hash}")
        self.data = {}
        
    def load_file(self, file_path: Path, file_type: str = 'auto') -> Optional[Any]:
        """Load a file and return its contents."""
        if not file_path.exists():
            return None
            
        try:
            if file_type == 'auto':
                if file_path.suffix == '.csv':
                    return pd.read_csv(file_path)
                elif file_path.suffix == '.json':
                    with open(file_path, 'r') as f:
                        return json.load(f)
                else:
                    with open(file_path, 'r') as f:
                        return f.read()
            elif file_type == 'csv':
                return pd.read_csv(file_path)
            elif file_type == 'json':
                with open(file_path, 'r') as f:
                    return json.load(f)
            elif file_type == 'text':
                with open(file_path, 'r') as f:
                    return f.read()
        except Exception as e:
            print(f"⚠️ Error loading {file_path}: {e}", file=sys.stderr)
            return None
    
    def get_file_path(self, relative_path: str) -> Path:
        """Get full path for a file relative to the run directory."""
        return self.base_path / relative_path
    
    def validate_data(self, data: Any, required_fields: List[str] = None) -> bool:
        """Validate that data contains required fields."""
        if data is None:
            return False
            
        if required_fields:
            if isinstance(data, dict):
                return all(field in data for field in required_fields)
            elif isinstance(data, pd.DataFrame):
                return all(field in data.columns for field in required_fields)
        
        return True
    
    @abstractmethod
    def load_data(self) -> Dict[str, Any]:
        """Load data specific to this agent type."""
        pass
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the loaded data."""
        summary = {
            'agent_type': self.__class__.__name__,
            'run_hash': self.run_hash,
            'files_loaded': [],
            'data_summary': {}
        }
        
        for key, value in self.data.items():
            if isinstance(value, pd.DataFrame):
                summary['files_loaded'].append(f"{key}.csv")
                summary['data_summary'][key] = {
                    'rows': len(value),
                    'columns': list(value.columns),
                    'shape': value.shape
                }
            elif isinstance(value, dict):
                summary['files_loaded'].append(f"{key}.json")
                summary['data_summary'][key] = {
                    'keys': list(value.keys()),
                    'type': 'json'
                }
            else:
                summary['files_loaded'].append(f"{key}.txt")
                summary['data_summary'][key] = {
                    'type': 'text',
                    'length': len(str(value))
                }
        
        return summary
