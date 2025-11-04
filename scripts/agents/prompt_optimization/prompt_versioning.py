#!/usr/bin/env python3
"""
Prompt Versioning System
Version: 1.0.0

Manages prompt versions and tracks performance for optimization.
"""

import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
import hashlib

class PromptVersionManager:
    """Manages prompt versions and tracks performance."""
    
    def __init__(self, storage_dir: str = "prompt_versions"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.versions_file = self.storage_dir / "versions.json"
        self.performance_file = self.storage_dir / "performance.json"
        
        # Load existing versions
        self.versions = self._load_versions()
        self.performance = self._load_performance()
    
    def _load_versions(self) -> Dict[str, Any]:
        """Load prompt versions from storage."""
        if self.versions_file.exists():
            try:
                with open(self.versions_file, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}
    
    def _load_performance(self) -> Dict[str, Any]:
        """Load performance tracking data."""
        if self.performance_file.exists():
            try:
                with open(self.performance_file, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}
    
    def _save_versions(self):
        """Save prompt versions to storage."""
        with open(self.versions_file, 'w') as f:
            json.dump(self.versions, f, indent=2)
    
    def _save_performance(self):
        """Save performance data to storage."""
        with open(self.performance_file, 'w') as f:
            json.dump(self.performance, f, indent=2)
    
    def _hash_prompt(self, system_prompt: str, user_prompt: str) -> str:
        """Generate hash for a prompt combination."""
        combined = f"{system_prompt}\n---\n{user_prompt}"
        return hashlib.md5(combined.encode()).hexdigest()[:12]
    
    def register_prompt_version(self, agent_type: str, system_prompt: str, user_prompt_template: str, 
                                version_name: Optional[str] = None, metadata: Dict[str, Any] = None) -> str:
        """
        Register a new prompt version.
        
        Returns:
            Version ID
        """
        prompt_hash = self._hash_prompt(system_prompt, user_prompt_template)
        
        if agent_type not in self.versions:
            self.versions[agent_type] = {}
        
        version_id = version_name or f"v{len(self.versions[agent_type]) + 1}"
        
        self.versions[agent_type][version_id] = {
            'hash': prompt_hash,
            'system_prompt': system_prompt,
            'user_prompt_template': user_prompt_template,
            'created_at': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        self._save_versions()
        return version_id
    
    def get_prompt_version(self, agent_type: str, version_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific prompt version."""
        return self.versions.get(agent_type, {}).get(version_id)
    
    def get_all_versions(self, agent_type: str) -> Dict[str, Dict[str, Any]]:
        """Get all versions for an agent type."""
        return self.versions.get(agent_type, {})
    
    def get_latest_version(self, agent_type: str) -> Optional[Dict[str, Any]]:
        """Get the latest prompt version for an agent."""
        agent_versions = self.versions.get(agent_type, {})
        if not agent_versions:
            return None
        
        # Get version with latest timestamp
        latest = max(agent_versions.items(), key=lambda x: x[1].get('created_at', ''))
        return latest[1]
    
    def record_performance(self, agent_type: str, version_id: str, run_hash: str, 
                          reward: float, metadata: Dict[str, Any] = None):
        """Record performance metrics for a prompt version."""
        key = f"{agent_type}:{version_id}"
        
        if key not in self.performance:
            self.performance[key] = {
                'agent_type': agent_type,
                'version_id': version_id,
                'runs': [],
                'avg_reward': 0.0,
                'total_runs': 0
            }
        
        run_record = {
            'run_hash': run_hash,
            'reward': reward,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        self.performance[key]['runs'].append(run_record)
        self.performance[key]['total_runs'] += 1
        
        # Recalculate average reward
        rewards = [r['reward'] for r in self.performance[key]['runs']]
        self.performance[key]['avg_reward'] = sum(rewards) / len(rewards)
        
        self._save_performance()
    
    def get_best_version(self, agent_type: str) -> Optional[str]:
        """Get the version ID with the best average reward."""
        agent_performance = {
            k: v for k, v in self.performance.items() 
            if v['agent_type'] == agent_type
        }
        
        if not agent_performance:
            return None
        
        best_key = max(agent_performance.items(), key=lambda x: x[1]['avg_reward'])
        return best_key[1]['version_id']
    
    def get_performance_summary(self, agent_type: str) -> Dict[str, Any]:
        """Get performance summary for an agent."""
        agent_performance = {
            k: v for k, v in self.performance.items() 
            if v['agent_type'] == agent_type
        }
        
        if not agent_performance:
            return {'total_versions': 0, 'best_version': None}
        
        best_key = max(agent_performance.items(), key=lambda x: x[1]['avg_reward'])
        best_version = best_key[1]
        
        return {
            'total_versions': len(agent_performance),
            'best_version': best_version['version_id'],
            'best_avg_reward': best_version['avg_reward'],
            'best_total_runs': best_version['total_runs'],
            'all_versions': {
                v['version_id']: {
                    'avg_reward': v['avg_reward'],
                    'total_runs': v['total_runs']
                }
                for v in agent_performance.values()
            }
        }

