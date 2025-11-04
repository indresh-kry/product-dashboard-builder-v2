#!/usr/bin/env python3
"""
Reward Function for Prompt Optimization
Version: 1.0.0

Defines reward functions for evaluating prompt quality in Phase 3 optimization.
"""

from typing import Dict, Any, List
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from recommendation_validator import RecommendationValidator

class PromptRewardFunction:
    """Reward function for evaluating prompt optimization quality."""
    
    def __init__(self):
        self.validator = RecommendationValidator()
    
    def calculate_reward(self, llm_response: Dict[str, Any], weights: Dict[str, float] = None) -> float:
        """
        Calculate reward for an LLM response based on output quality.
        
        Args:
            llm_response: The LLM response dictionary containing parsed_response
            weights: Optional weights for different score components
            
        Returns:
            Reward score between 0.0 and 1.0
        """
        if weights is None:
            weights = {
                'specificity': 0.4,
                'actionability': 0.4,
                'data_support': 0.2
            }
        
        if not llm_response.get('success') or not llm_response.get('parsed_response'):
            return 0.0
        
        parsed = llm_response['parsed_response']
        
        # Calculate scores for recommendations
        recommendations = parsed.get('recommendations', [])
        if not recommendations:
            return 0.1  # Low reward if no recommendations
        
        # Validate all recommendations
        total_specificity = 0.0
        total_actionability = 0.0
        total_data_support = 0.0
        valid_count = 0
        
        for rec in recommendations:
            validation = self.validator.validate_recommendation(rec)
            if validation['is_valid']:
                total_specificity += validation['specificity_score']
                total_actionability += validation['actionability_score']
                total_data_support += validation['data_support_score']
                valid_count += 1
        
        if valid_count == 0:
            return 0.1  # Low reward if no valid recommendations
        
        # Calculate average scores
        avg_specificity = total_specificity / valid_count
        avg_actionability = total_actionability / valid_count
        avg_data_support = total_data_support / valid_count
        
        # Calculate weighted reward (normalize from 0-10 to 0-1)
        reward = (
            (avg_specificity / 10.0) * weights['specificity'] +
            (avg_actionability / 10.0) * weights['actionability'] +
            (avg_data_support / 10.0) * weights['data_support']
        )
        
        # Bonus for having insights
        if parsed.get('insights'):
            reward += 0.05  # Small bonus
        
        # Bonus for having data quality assessment
        if parsed.get('data_quality'):
            reward += 0.05  # Small bonus
        
        # Cap at 1.0
        return min(reward, 1.0)
    
    def calculate_batch_reward(self, responses: List[Dict[str, Any]], weights: Dict[str, float] = None) -> float:
        """
        Calculate average reward across multiple responses.
        
        Args:
            responses: List of LLM response dictionaries
            weights: Optional weights for different score components
            
        Returns:
            Average reward score between 0.0 and 1.0
        """
        if not responses:
            return 0.0
        
        total_reward = sum(self.calculate_reward(resp, weights) for resp in responses)
        return total_reward / len(responses)

