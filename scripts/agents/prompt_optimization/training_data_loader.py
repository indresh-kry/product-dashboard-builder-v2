#!/usr/bin/env python3
"""
Training Data Loader for Prompt Optimization
Version: 1.0.0

Loads historical run data for training prompt optimization models.
"""

import json
import os
from typing import Dict, Any, List, Optional
from pathlib import Path
import glob

class TrainingDataLoader:
    """Loads historical run data for training."""
    
    def __init__(self, run_logs_dir: str = "run_logs"):
        self.run_logs_dir = Path(run_logs_dir)
    
    def load_historical_runs(self, agent_type: Optional[str] = None, 
                          limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Load historical run data.
        
        Args:
            agent_type: Optional filter for specific agent type
            limit: Optional limit on number of runs to load
            
        Returns:
            List of run data dictionaries
        """
        runs = []
        
        if not self.run_logs_dir.exists():
            return runs
        
        # Find all run directories
        run_dirs = sorted(self.run_logs_dir.glob("*"), key=lambda x: x.name, reverse=True)
        
        if limit:
            run_dirs = run_dirs[:limit]
        
        for run_dir in run_dirs:
            if not run_dir.is_dir():
                continue
            
            run_hash = run_dir.name
            insights_file = run_dir / "outputs" / "insights" / "agentic_insights.json"
            
            if not insights_file.exists():
                continue
            
            try:
                with open(insights_file, 'r') as f:
                    run_data = json.load(f)
                
                # Extract agent-specific data
                agent_results = run_data.get('agent_results', {})
                
                if agent_type:
                    # Filter for specific agent
                    if agent_type in agent_results:
                        agent_result = agent_results[agent_type]
                        if 'llm_response' in agent_result:
                            runs.append({
                                'run_hash': run_hash,
                                'agent_type': agent_type,
                                'llm_response': agent_result['llm_response'],
                                'data': agent_result.get('data', {}),
                                'timestamp': run_data.get('timestamp', ''),
                                'metadata': run_data.get('run_metadata', {})
                            })
                else:
                    # Load all agents
                    for atype, agent_result in agent_results.items():
                        if 'llm_response' in agent_result:
                            runs.append({
                                'run_hash': run_hash,
                                'agent_type': atype,
                                'llm_response': agent_result['llm_response'],
                                'data': agent_result.get('data', {}),
                                'timestamp': run_data.get('timestamp', ''),
                                'metadata': run_data.get('run_metadata', {})
                            })
            
            except Exception as e:
                # Skip runs that can't be loaded
                continue
        
        return runs
    
    def get_training_data_for_agent(self, agent_type: str, limit: Optional[int] = 20) -> List[Dict[str, Any]]:
        """Get training data for a specific agent type."""
        return self.load_historical_runs(agent_type=agent_type, limit=limit)
    
    def get_all_training_data(self, limit: Optional[int] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Get all training data organized by agent type."""
        all_data = {}
        
        agent_types = ['daily_metrics', 'user_segmentation', 'geographic', 
                      'cohort_retention', 'revenue_optimization', 'data_quality']
        
        for agent_type in agent_types:
            all_data[agent_type] = self.get_training_data_for_agent(agent_type, limit=limit)
        
        return all_data

