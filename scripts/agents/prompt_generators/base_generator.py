#!/usr/bin/env python3
"""
Base Prompt Generator
Version: 2.0.0
Last Updated: 2025-10-23

Base class for all prompt generators.
Provides common functionality for generating analysis prompts.
"""

import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime

class BasePromptGenerator(ABC):
    """Base class for all prompt generators."""
    
    def __init__(self, agent_type: str):
        self.agent_type = agent_type
        
    def format_data_for_prompt(self, data: Dict[str, Any]) -> str:
        """Format data for inclusion in prompts."""
        if not data:
            return "No data available for analysis."
        
        formatted_sections = []
        
        for key, value in data.items():
            if key == 'summary':
                continue
                
            if hasattr(value, 'head'):  # DataFrame
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{value.head().to_string()}")
            elif isinstance(value, dict):
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{json.dumps(value, indent=2)}")
            else:
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{str(value)}")
        
        return "\n\n".join(formatted_sections)
    
    def get_analysis_instructions(self) -> str:
        """Get standard analysis instructions."""
        return """
Please analyze the provided data and generate insights. Focus on:
1. Key trends and patterns
2. Significant changes or anomalies with evidence
3. Actionable recommendations in simple language with no jargon 
4. Data quality observations

Provide your analysis in a structured JSON format as specified in the system prompt.
"""
    
    def get_context_info(self, run_metadata: Dict[str, Any]) -> str:
        """Get context information for the analysis."""
        context_parts = []
        
        if 'app_filter' in run_metadata:
            context_parts.append(f"App: {run_metadata['app_filter']}")
        
        if 'date_start' in run_metadata and 'date_end' in run_metadata:
            context_parts.append(f"Date Range: {run_metadata['date_start']} to {run_metadata['date_end']}")
        
        if 'raw_data_limit' in run_metadata:
            context_parts.append(f"Data Limit: {run_metadata['raw_data_limit']}")
        
        return " | ".join(context_parts) if context_parts else "No context available"
    
    @abstractmethod
    def generate_prompt(self, data: Dict[str, Any], run_metadata: Dict[str, Any]) -> str:
        """Generate the analysis prompt for this agent type."""
        pass
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """Get the system prompt for this agent type."""
        pass
