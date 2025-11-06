#!/usr/bin/env python3
"""
Base Agent Framework
Version: 2.0.0
Last Updated: 2025-10-23

Base classes for the agentic LLM framework.
Provides the foundation for all specialized agents.
"""

import os
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime

class BaseAgent(ABC):
    """Base class for all LLM agents."""
    
    def __init__(self, agent_type: str, config: Dict[str, Any]):
        self.agent_type = agent_type
        self.config = config
        self.run_hash = os.environ.get('RUN_HASH', 'unknown')
        
    @abstractmethod
    def load_data(self, run_hash: str) -> Dict[str, Any]:
        """Load data specific to this agent type."""
        pass
    
    @abstractmethod
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate the analysis prompt for this agent."""
        pass
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """Get the system prompt for this agent type."""
        pass
    
    def analyze(self, run_hash: str, run_metadata: Dict[str, Any], charts_info: Optional[str] = None) -> Dict[str, Any]:
        """Main analysis workflow for the agent."""
        try:
            # Load data
            data = self.load_data(run_hash)
            
            # Generate prompt
            prompt = self.generate_prompt(data, run_metadata, charts_info)
            
            # Get system prompt
            system_prompt = self.get_system_prompt()
            
            # Return structured response
            return {
                'agent_type': self.agent_type,
                'data': data,
                'prompt': prompt,
                'system_prompt': system_prompt,
                'run_hash': run_hash,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'agent_type': self.agent_type,
                'error': str(e),
                'run_hash': run_hash,
                'timestamp': datetime.now().isoformat()
            }

class LLMAgent(BaseAgent):
    """LLM-powered agent that can call external LLM services."""
    
    def __init__(self, agent_type: str, config: Dict[str, Any], llm_client=None):
        super().__init__(agent_type, config)
        self.llm_client = llm_client
        self.data_loader = None
        self.prompt_generator = None
        
    def load_data(self, run_hash: str) -> Dict[str, Any]:
        """Load data using the attached data loader."""
        if self.data_loader:
            return self.data_loader.load_data()
        return {}
    
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any], charts_info: Optional[str] = None) -> str:
        """Generate prompt using the attached prompt generator."""
        if self.prompt_generator:
            return self.prompt_generator.generate_prompt(data, run_metadata, charts_info)
        return "No prompt generator available"
    
    def get_system_prompt(self) -> str:
        """Get system prompt using the attached prompt generator."""
        if self.prompt_generator:
            return self.prompt_generator.get_system_prompt()
        return "No system prompt available"
        
    def analyze_with_llm(self, run_hash: str, run_metadata: Dict[str, Any], charts_info: Optional[str] = None) -> Dict[str, Any]:
        """Analyze with LLM integration and recommendation validation."""
        try:
            # Get base analysis
            analysis = self.analyze(run_hash, run_metadata, charts_info)
            
            if 'error' in analysis:
                return analysis
            
            # Phase 3: Get optimized prompts if available
            prompt_version_id = 'current'
            system_prompt = analysis['system_prompt']
            
            try:
                from .prompt_optimization import Phase3Integration
                phase3 = Phase3Integration()
                optimized_prompts = phase3.get_optimized_prompt(
                    self.agent_type,
                    system_prompt,
                    analysis['prompt']
                )
                
                if optimized_prompts.get('optimized'):
                    system_prompt = optimized_prompts['system_prompt']
                    prompt_version_id = optimized_prompts['version_id']
                    analysis['prompt_version_id'] = prompt_version_id
                    analysis['using_optimized_prompt'] = True
            except Exception as phase3_error:
                # Continue with default prompts if Phase 3 fails
                pass
            
            # Call LLM if client is available
            if self.llm_client:
                llm_response = self.llm_client.call(
                    prompt=analysis['prompt'],
                    system_prompt=system_prompt
                )
                analysis['llm_response'] = llm_response
                
                # Phase 3: Record performance for optimization
                try:
                    from .prompt_optimization import Phase3Integration
                    phase3 = Phase3Integration()
                    phase3.record_agent_performance(
                        self.agent_type,
                        run_hash,
                        llm_response,
                        prompt_version_id
                    )
                except Exception:
                    # Don't fail if Phase 3 tracking fails
                    pass
                
                # Validate and filter recommendations if response was successful
                if llm_response.get('success') and llm_response.get('parsed_response'):
                    try:
                        from .recommendation_validator import RecommendationValidator
                        # Enable strict validation mode for better quality recommendations
                        import os
                        strict_validation = os.environ.get('STRICT_VALIDATION', 'true').lower() == 'true'
                        validator = RecommendationValidator(strict_mode=strict_validation)
                        parsed = llm_response['parsed_response']
                        
                        # Filter recommendations
                        if 'recommendations' in parsed and isinstance(parsed['recommendations'], list):
                            filtered_recs, filtered_out_recs = validator.filter_recommendations(parsed['recommendations'])
                            parsed['recommendations'] = filtered_recs
                            parsed['recommendations_filtered'] = {
                                'filtered_count': len(filtered_out_recs),
                                'remaining_count': len(filtered_recs),
                                'filtered_out': filtered_out_recs
                            }
                        
                        # Validate insights (add scores, but don't filter)
                        if 'insights' in parsed and isinstance(parsed['insights'], list):
                            parsed['insights'] = validator.validate_insights(parsed['insights'])
                        
                    except Exception as validation_error:
                        # Don't fail analysis if validation fails
                        analysis['llm_response']['validation_error'] = str(validation_error)
            else:
                analysis['llm_response'] = None
                
            return analysis
            
        except Exception as e:
            return {
                'agent_type': self.agent_type,
                'error': str(e),
                'run_hash': run_hash,
                'timestamp': datetime.now().isoformat()
            }
