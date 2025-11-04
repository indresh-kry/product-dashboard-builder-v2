#!/usr/bin/env python3
"""
Automatic Prompt Optimization (APO) Framework
Version: 1.0.0

Implements APO for prompt optimization, with optional Agent Lightning integration.
"""

import json
import sys
from typing import Dict, Any, List, Optional, Callable
from pathlib import Path
import random

# Try to import Agent Lightning (optional)
try:
    from agent_lightning import Agent, APOAlgorithm
    AGENT_LIGHTNING_AVAILABLE = True
except ImportError:
    AGENT_LIGHTNING_AVAILABLE = False
    print("⚠️ Agent Lightning not available. Using fallback APO implementation.", file=sys.stderr)

from .reward_function import PromptRewardFunction
from .prompt_versioning import PromptVersionManager

class APOPromptOptimizer:
    """Automatic Prompt Optimization framework."""
    
    def __init__(self, agent_type: str, use_agent_lightning: bool = False):
        self.agent_type = agent_type
        self.use_agent_lightning = use_agent_lightning and AGENT_LIGHTNING_AVAILABLE
        self.reward_function = PromptRewardFunction()
        self.version_manager = PromptVersionManager()
        
        if self.use_agent_lightning:
            print(f"✅ Using Agent Lightning APO for {agent_type}", file=sys.stderr)
        else:
            print(f"⚠️ Using fallback APO implementation for {agent_type}", file=sys.stderr)
    
    def optimize_prompts(self, initial_prompts: Dict[str, str], 
                        training_data: List[Dict[str, Any]],
                        optimization_steps: int = 100,
                        temperature_variations: float = 0.1) -> Dict[str, Any]:
        """
        Optimize prompts using APO algorithm.
        
        Args:
            initial_prompts: Dict with 'system_prompt' and 'user_prompt_template'
            training_data: List of historical run data for training
            optimization_steps: Number of optimization iterations
            temperature_variations: Temperature for prompt variations
            
        Returns:
            Optimized prompts and performance metrics
        """
        if self.use_agent_lightning:
            return self._optimize_with_agent_lightning(
                initial_prompts, training_data, optimization_steps
            )
        else:
            return self._optimize_fallback(
                initial_prompts, training_data, optimization_steps, temperature_variations
            )
    
    def _optimize_with_agent_lightning(self, initial_prompts: Dict[str, str],
                                      training_data: List[Dict[str, Any]],
                                      optimization_steps: int) -> Dict[str, Any]:
        """Optimize using Agent Lightning library."""
        if not AGENT_LIGHTNING_AVAILABLE:
            # Fallback if Agent Lightning became unavailable
            return self._optimize_fallback(initial_prompts, training_data, optimization_steps)
        
        # Register initial version
        initial_version = self.version_manager.register_prompt_version(
            self.agent_type,
            initial_prompts['system_prompt'],
            initial_prompts.get('user_prompt_template', ''),
            version_name='initial',
            metadata={'source': 'initial_prompt', 'optimization_method': 'agent_lightning'}
        )
        
        # Create Agent Lightning compatible agent wrapper
        class AgentLightningWrapper(Agent):
            """Wrapper to make our agent compatible with Agent Lightning."""
            
            def __init__(self, agent_type, reward_function, version_manager):
                self.agent_type = agent_type
                self.reward_function = reward_function
                self.version_manager = version_manager
                # Note: In a real implementation, you'd inject the LLM client here
                
            def __call__(self, prompt: str, data: Dict[str, Any]) -> Dict[str, Any]:
                """
                Execute the agent with given prompt.
                This is a simplified version - in production, you'd call the actual LLM.
                """
                # For optimization, we'll use the training data responses
                # In a real implementation, you'd make actual LLM calls
                return data.get('llm_response', {})
            
            def reward_function(self, response: Dict[str, Any]) -> float:
                """Calculate reward for a response."""
                return self.reward_function.calculate_reward(response)
        
        # Create wrapper agent
        agent = AgentLightningWrapper(self.agent_type, self.reward_function, self.version_manager)
        
        # Initialize APO algorithm
        apo = APOAlgorithm(
            agent=agent,
            reward_function=lambda r: agent.reward_function(r),
            optimization_steps=optimization_steps
        )
        
        # Prepare training data in Agent Lightning format
        # Agent Lightning expects (prompt, data) tuples
        training_pairs = [
            (initial_prompts['user_prompt_template'], data_sample)
            for data_sample in training_data[:20]  # Limit for efficiency
        ]
        
        # Run optimization
        print(f"🚀 Running Agent Lightning APO optimization ({optimization_steps} steps)...", file=sys.stderr)
        
        try:
            optimized_prompts = apo.optimize(
                initial_prompts=initial_prompts,
                training_data=training_pairs
            )
            
            # Extract best prompts (format depends on Agent Lightning API)
            if isinstance(optimized_prompts, dict):
                best_prompts = optimized_prompts.get('optimized_prompts', initial_prompts)
                best_reward = optimized_prompts.get('best_reward', 0.0)
                initial_reward = optimized_prompts.get('initial_reward', 0.0)
            else:
                # Fallback if format is different
                best_prompts = initial_prompts
                best_reward = 0.0
                initial_reward = 0.0
            
            # Register optimized version
            best_version = self.version_manager.register_prompt_version(
                self.agent_type,
                best_prompts.get('system_prompt', initial_prompts['system_prompt']),
                best_prompts.get('user_prompt_template', initial_prompts.get('user_prompt_template', '')),
                version_name='agent_lightning_optimized',
                metadata={
                    'source': 'agent_lightning_apo',
                    'optimization_steps': optimization_steps,
                    'initial_reward': initial_reward,
                    'best_reward': best_reward
                }
            )
            
            return {
                'optimized_prompts': best_prompts,
                'best_version': best_version,
                'best_reward': best_reward,
                'initial_reward': initial_reward,
                'improvement': best_reward - initial_reward,
                'optimization_method': 'agent_lightning',
                'total_steps': optimization_steps
            }
            
        except Exception as e:
            print(f"⚠️ Agent Lightning optimization failed: {e}. Falling back to built-in APO.", file=sys.stderr)
            return self._optimize_fallback(initial_prompts, training_data, optimization_steps)
    
    def _optimize_fallback(self, initial_prompts: Dict[str, str],
                          training_data: List[Dict[str, Any]],
                          optimization_steps: int,
                          temperature_variations: float) -> Dict[str, Any]:
        """
        Fallback APO implementation that doesn't require Agent Lightning.
        Uses simple prompt variation and reward-based selection.
        """
        # Register initial version
        initial_version = self.version_manager.register_prompt_version(
            self.agent_type,
            initial_prompts['system_prompt'],
            initial_prompts.get('user_prompt_template', ''),
            version_name='initial',
            metadata={'source': 'initial_prompt'}
        )
        
        # Test initial version on training data
        initial_reward = self._evaluate_on_training_data(
            initial_prompts, training_data, initial_version
        )
        
        best_prompts = initial_prompts.copy()
        best_reward = initial_reward
        best_version = initial_version
        
        optimization_history = [{
            'step': 0,
            'version': initial_version,
            'reward': initial_reward
        }]
        
        # Generate prompt variations and test
        for step in range(1, optimization_steps + 1):
            # Generate variation
            variation = self._generate_prompt_variation(
                best_prompts, temperature_variations
            )
            
            # Register variation
            var_version = self.version_manager.register_prompt_version(
                self.agent_type,
                variation['system_prompt'],
                variation.get('user_prompt_template', ''),
                version_name=f'step_{step}',
                metadata={'source': 'apo_optimization', 'step': step}
            )
            
            # Evaluate variation
            var_reward = self._evaluate_on_training_data(
                variation, training_data, var_version
            )
            
            optimization_history.append({
                'step': step,
                'version': var_version,
                'reward': var_reward
            })
            
            # Update best if this variation is better
            if var_reward > best_reward:
                best_prompts = variation.copy()
                best_reward = var_reward
                best_version = var_version
                print(f"✅ Step {step}: New best reward {best_reward:.3f} (version: {best_version})", file=sys.stderr)
        
        return {
            'optimized_prompts': best_prompts,
            'best_version': best_version,
            'best_reward': best_reward,
            'initial_reward': initial_reward,
            'improvement': best_reward - initial_reward,
            'optimization_history': optimization_history,
            'total_steps': optimization_steps
        }
    
    def _evaluate_on_training_data(self, prompts: Dict[str, str],
                                   training_data: List[Dict[str, Any]],
                                   version_id: str) -> float:
        """Evaluate prompts on training data and return average reward."""
        if not training_data:
            return 0.0
        
        # For now, we'll simulate evaluation
        # In a real implementation, this would run the prompts through the LLM
        # and calculate rewards based on actual responses
        
        # Simplified: return a sample reward based on prompt quality
        # In production, this would actually run the LLM with these prompts
        rewards = []
        
        for data_sample in training_data[:10]:  # Limit to 10 samples for efficiency
            # In real implementation, would call LLM here
            # For now, use a simple heuristic
            reward = self.reward_function.calculate_reward(
                data_sample.get('llm_response', {})
            )
            rewards.append(reward)
            
            # Record performance
            self.version_manager.record_performance(
                self.agent_type,
                version_id,
                data_sample.get('run_hash', 'unknown'),
                reward
            )
        
        return sum(rewards) / len(rewards) if rewards else 0.0
    
    def _generate_prompt_variation(self, base_prompts: Dict[str, str],
                                  temperature: float) -> Dict[str, Any]:
        """
        Generate a variation of the base prompts.
        This is a simplified version - in production, this would use more sophisticated techniques.
        """
        variation = base_prompts.copy()
        
        # Simple variation: modify system prompt slightly
        system_prompt = base_prompts['system_prompt']
        
        # Add minor variations (e.g., emphasize different aspects)
        variations = [
            system_prompt.replace("CRITICAL CONSTRAINTS", "IMPORTANT CONSTRAINTS"),
            system_prompt.replace("REQUIRED format", "RECOMMENDED format"),
            system_prompt + "\n\nRemember to be specific and data-driven in your analysis.",
        ]
        
        if variations:
            variation['system_prompt'] = random.choice(variations)
        
        return variation
    
    def load_optimized_prompt(self, version_id: Optional[str] = None) -> Optional[Dict[str, str]]:
        """Load optimized prompt version."""
        if version_id is None:
            version_id = self.version_manager.get_best_version(self.agent_type)
        
        if version_id is None:
            return None
        
        version_data = self.version_manager.get_prompt_version(self.agent_type, version_id)
        if version_data:
            return {
                'system_prompt': version_data['system_prompt'],
                'user_prompt_template': version_data['user_prompt_template'],
                'version_id': version_id
            }
        
        return None

