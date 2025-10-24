"""
Agentic LLM Framework
Version: 2.0.0
Last Updated: 2025-10-23

This package provides an agentic framework for LLM-based analysis.
Replaces individual child LLM scripts with a unified, scalable agent system.
"""

from .base_agent import BaseAgent, LLMAgent
from .llm_client import LLMClient

__all__ = ['BaseAgent', 'LLMAgent', 'LLMClient']
