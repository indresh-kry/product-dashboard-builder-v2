#!/usr/bin/env python3
"""
Phase 3 Integration
Version: 1.0.0

Integrates prompt optimization into the agent workflow.
"""

import os
import sys
from typing import Dict, Any, Optional
from pathlib import Path

from .apo_optimizer import APOPromptOptimizer
from .training_data_loader import TrainingDataLoader
from .prompt_versioning import PromptVersionManager
from .reward_function import PromptRewardFunction

class Phase3Integration:
    """Integration layer for Phase 3 prompt optimization."""
    
    def __init__(self, enable_optimization: bool = True):
        self.enable_optimization = enable_optimization and os.environ.get('ENABLE_PHASE3_OPTIMIZATION', '0') == '1'
        self.version_manager = PromptVersionManager()
        self.reward_function = PromptRewardFunction()
        
        if self.enable_optimization:
            print("✅ Phase 3 optimization enabled", file=sys.stderr)
        else:
            print("⚠️ Phase 3 optimization disabled (set ENABLE_PHASE3_OPTIMIZATION=1 to enable)", file=sys.stderr)
    
    def get_optimized_prompt(self, agent_type: str, 
                           current_system_prompt: str,
                           current_user_prompt_template: str) -> Dict[str, str]:
        """
        Get optimized prompt version for an agent, or return current if optimization disabled.
        
        Returns:
            Dict with 'system_prompt' and 'user_prompt_template'
        """
        if not self.enable_optimization:
            return {
                'system_prompt': current_system_prompt,
                'user_prompt_template': current_user_prompt_template,
                'version_id': 'current',
                'optimized': False
            }
        
        # Try to load best optimized version
        optimizer = APOPromptOptimizer(agent_type, use_agent_lightning=False)
        optimized = optimizer.load_optimized_prompt()
        
        if optimized:
            return {
                'system_prompt': optimized['system_prompt'],
                'user_prompt_template': optimized['user_prompt_template'],
                'version_id': optimized['version_id'],
                'optimized': True
            }
        
        # Fall back to current
        return {
            'system_prompt': current_system_prompt,
            'user_prompt_template': current_user_prompt_template,
            'version_id': 'current',
            'optimized': False
        }
    
    def record_agent_performance(self, agent_type: str, run_hash: str, 
                                 llm_response: Dict[str, Any],
                                 prompt_version_id: str = 'current'):
        """Record agent performance for optimization tracking."""
        if not self.enable_optimization:
            return
        
        # Calculate reward
        reward = self.reward_function.calculate_reward(llm_response)
        
        # Record performance
        self.version_manager.record_performance(
            agent_type,
            prompt_version_id,
            run_hash,
            reward,
            metadata={
                'success': llm_response.get('success', False),
                'has_recommendations': bool(
                    llm_response.get('parsed_response', {}).get('recommendations')
                )
            }
        )
    
    def run_optimization(self, agent_type: str, optimization_steps: int = 50):
        """
        Run optimization for a specific agent type.
        
        This should be run separately, not during normal agent execution.
        """
        if not self.enable_optimization:
            print(f"⚠️ Optimization disabled. Enable with ENABLE_PHASE3_OPTIMIZATION=1", file=sys.stderr)
            return None
        
        print(f"🚀 Starting optimization for {agent_type}...", file=sys.stderr)
        
        # Load training data
        data_loader = TrainingDataLoader()
        training_data = data_loader.get_training_data_for_agent(agent_type, limit=20)
        
        if not training_data:
            print(f"⚠️ No training data found for {agent_type}", file=sys.stderr)
            return None
        
        print(f"📊 Loaded {len(training_data)} training samples", file=sys.stderr)
        
        # Get current prompts from prompt generator
        # This would need to be passed in or loaded from the prompt generator
        # For now, we'll use a placeholder
        current_prompts = {
            'system_prompt': '',  # Would load from prompt generator
            'user_prompt_template': ''
        }
        
        # Run optimization
        optimizer = APOPromptOptimizer(agent_type, use_agent_lightning=False)
        result = optimizer.optimize_prompts(
            current_prompts,
            training_data,
            optimization_steps=optimization_steps
        )
        
        print(f"✅ Optimization complete for {agent_type}", file=sys.stderr)
        print(f"   Best reward: {result['best_reward']:.3f} (improvement: {result['improvement']:.3f})", file=sys.stderr)
        
        return result

