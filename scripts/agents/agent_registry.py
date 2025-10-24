#!/usr/bin/env python3
"""
Agent Registry
Version: 2.0.0
Last Updated: 2025-10-23

Configuration-driven agent registry.
Manages agent types, configurations, and instantiation.
"""

import json
from typing import Dict, Any, List, Optional
from pathlib import Path

from .base_agent import BaseAgent, LLMAgent
from .llm_client import LLMClient
from .data_loaders import (
    DailyMetricsDataLoader,
    UserSegmentationDataLoader,
    GeographicDataLoader,
    CohortRetentionDataLoader,
    RevenueOptimizationDataLoader,
    DataQualityDataLoader
)
from .prompt_generators import (
    DailyMetricsPromptGenerator,
    UserSegmentationPromptGenerator,
    GeographicPromptGenerator,
    CohortRetentionPromptGenerator,
    RevenueOptimizationPromptGenerator,
    DataQualityPromptGenerator
)

class AgentRegistry:
    """Registry for managing agent types and configurations."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "scripts/config/agent_config.json"
        self.agent_configs = self._load_config()
        self.llm_client = None
        
    def _load_config(self) -> Dict[str, Any]:
        """Load agent configurations from file."""
        try:
            if Path(self.config_path).exists():
                with open(self.config_path, 'r') as f:
                    return json.load(f)
            else:
                return self._get_default_config()
        except Exception as e:
            print(f"⚠️ Error loading agent config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default agent configuration."""
        return {
            "agents": {
                "daily_metrics": {
                    "enabled": True,
                    "data_loader": "DailyMetricsDataLoader",
                    "prompt_generator": "DailyMetricsPromptGenerator",
                    "llm_enabled": True,
                    "priority": 1
                },
                "user_segmentation": {
                    "enabled": True,
                    "data_loader": "UserSegmentationDataLoader",
                    "prompt_generator": "UserSegmentationPromptGenerator",
                    "llm_enabled": True,
                    "priority": 2
                },
                "geographic": {
                    "enabled": True,
                    "data_loader": "GeographicDataLoader",
                    "prompt_generator": "GeographicPromptGenerator",
                    "llm_enabled": True,
                    "priority": 3
                },
                "cohort_retention": {
                    "enabled": True,
                    "data_loader": "CohortRetentionDataLoader",
                    "prompt_generator": "CohortRetentionPromptGenerator",
                    "llm_enabled": True,
                    "priority": 4
                },
                "revenue_optimization": {
                    "enabled": True,
                    "data_loader": "RevenueOptimizationDataLoader",
                    "prompt_generator": "RevenueOptimizationPromptGenerator",
                    "llm_enabled": True,
                    "priority": 5
                },
                "data_quality": {
                    "enabled": True,
                    "data_loader": "DataQualityDataLoader",
                    "prompt_generator": "DataQualityPromptGenerator",
                    "llm_enabled": True,
                    "priority": 6
                }
            },
            "llm": {
                "model": "gpt-4",
                "temperature": 0.3,
                "max_tokens": 1000
            }
        }
    
    def get_llm_client(self) -> LLMClient:
        """Get or create LLM client."""
        if self.llm_client is None:
            self.llm_client = LLMClient()
        return self.llm_client
    
    def get_enabled_agents(self) -> List[str]:
        """Get list of enabled agent types."""
        return [
            agent_type for agent_type, config in self.agent_configs.get("agents", {}).items()
            if config.get("enabled", True)
        ]
    
    def get_agent_priority(self, agent_type: str) -> int:
        """Get priority for an agent type."""
        return self.agent_configs.get("agents", {}).get(agent_type, {}).get("priority", 999)
    
    def create_agent(self, agent_type: str, run_hash: str) -> Optional[BaseAgent]:
        """Create an agent instance."""
        if agent_type not in self.agent_configs.get("agents", {}):
            return None
        
        config = self.agent_configs["agents"][agent_type]
        
        # Create data loader
        data_loader_class = self._get_data_loader_class(config.get("data_loader"))
        if not data_loader_class:
            return None
        
        data_loader = data_loader_class(run_hash)
        
        # Create prompt generator
        prompt_generator_class = self._get_prompt_generator_class(config.get("prompt_generator"))
        if not prompt_generator_class:
            return None
        
        prompt_generator = prompt_generator_class()
        
        # Create agent
        if config.get("llm_enabled", True):
            llm_client = self.get_llm_client()
            agent = LLMAgent(agent_type, config, llm_client)
        else:
            agent = BaseAgent(agent_type, config)
        
        # Attach components
        agent.data_loader = data_loader
        agent.prompt_generator = prompt_generator
        
        return agent
    
    def _get_data_loader_class(self, loader_name: str):
        """Get data loader class by name."""
        loaders = {
            "DailyMetricsDataLoader": DailyMetricsDataLoader,
            "UserSegmentationDataLoader": UserSegmentationDataLoader,
            "GeographicDataLoader": GeographicDataLoader,
            "CohortRetentionDataLoader": CohortRetentionDataLoader,
            "RevenueOptimizationDataLoader": RevenueOptimizationDataLoader,
            "DataQualityDataLoader": DataQualityDataLoader
        }
        return loaders.get(loader_name)
    
    def _get_prompt_generator_class(self, generator_name: str):
        """Get prompt generator class by name."""
        generators = {
            "DailyMetricsPromptGenerator": DailyMetricsPromptGenerator,
            "UserSegmentationPromptGenerator": UserSegmentationPromptGenerator,
            "GeographicPromptGenerator": GeographicPromptGenerator,
            "CohortRetentionPromptGenerator": CohortRetentionPromptGenerator,
            "RevenueOptimizationPromptGenerator": RevenueOptimizationPromptGenerator,
            "DataQualityPromptGenerator": DataQualityPromptGenerator
        }
        return generators.get(generator_name)
    
    def get_agent_summary(self) -> Dict[str, Any]:
        """Get summary of available agents."""
        summary = {
            "total_agents": len(self.agent_configs.get("agents", {})),
            "enabled_agents": len(self.get_enabled_agents()),
            "agents": {}
        }
        
        for agent_type, config in self.agent_configs.get("agents", {}).items():
            summary["agents"][agent_type] = {
                "enabled": config.get("enabled", True),
                "priority": config.get("priority", 999),
                "llm_enabled": config.get("llm_enabled", True)
            }
        
        return summary
