#!/usr/bin/env python3
"""
Base Prompt Generator
Version: 2.1.0
Last Updated: 2025-11-03

Base class for all prompt generators.
Provides common functionality for generating analysis prompts.
Enhanced with data pre-processing and token optimization.
"""

import json
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

class BasePromptGenerator(ABC):
    """Base class for all prompt generators."""
    
    def __init__(self, agent_type: str):
        self.agent_type = agent_type
        self.example_file_path = Path(__file__).parent / f"examples_{agent_type}.txt"
        
    def format_data_for_prompt(self, data: Dict[str, Any], use_summaries: bool = True, max_rows: int = 100) -> str:
        """
        Format data for inclusion in prompts with token optimization.
        
        Args:
            data: Data dict containing DataFrames
            use_summaries: If True, include statistical summaries and limit full data
            max_rows: Maximum rows of full data to include (to save tokens)
        """
        if not data:
            return "No data available for analysis."
        
        if not PANDAS_AVAILABLE:
            # Fallback to simple formatting if pandas not available
            return self._format_data_simple(data)
        
        formatted_sections = []
        
        for key, value in data.items():
            if key == 'summary':
                continue
            
            if isinstance(value, pd.DataFrame) and len(value) > 0:
                if use_summaries:
                    # Use optimized format with summaries
                    formatted = self._format_dataframe_with_summary(value, key, max_rows=max_rows)
                else:
                    # Legacy format - limited rows
                    formatted = f"**{key.replace('_', ' ').title()}:**\n{value.head(max_rows).to_string(index=False)}"
                formatted_sections.append(formatted)
            elif isinstance(value, dict):
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{json.dumps(value, indent=2)}")
            else:
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{str(value)}")
        
        return "\n\n".join(formatted_sections)
    
    def _format_data_simple(self, data: Dict[str, Any]) -> str:
        """Simple formatting fallback when pandas not available."""
        formatted_sections = []
        for key, value in data.items():
            if key == 'summary':
                continue
            if hasattr(value, 'head'):
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{value.head().to_string()}")
            elif isinstance(value, dict):
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{json.dumps(value, indent=2)}")
            else:
                formatted_sections.append(f"**{key.replace('_', ' ').title()}:**\n{str(value)}")
        return "\n\n".join(formatted_sections)
    
    def _format_dataframe_with_summary(self, df: pd.DataFrame, key: str, max_rows: int = 100) -> str:
        """
        Format DataFrame with statistical summaries (token-optimized).
        
        Args:
            df: DataFrame to format
            key: Key name for the data
            max_rows: Maximum rows to include in full dataset
        """
        sections = [f"**{key.replace('_', ' ').title()}:**"]
        
        # Basic summary
        sections.append(f"- Total records: {len(df)}")
        sections.append(f"- Columns: {', '.join(df.columns.tolist())}")
        
        # Date range if date column exists
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if date_cols:
            date_col = date_cols[0]
            sections.append(f"- Date range: {df[date_col].min()} to {df[date_col].max()}")
        
        # Statistical summaries for numeric columns (token-optimized)
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            summary_stats = df[numeric_cols].describe().T
            # Only include mean, min, max to save tokens
            sections.append("\n**Key Metrics:**")
            for col in numeric_cols[:10]:  # Limit to top 10 numeric columns
                mean_val = df[col].mean()
                sections.append(f"- {col}: avg={mean_val:.2f}, min={df[col].min():.2f}, max={df[col].max():.2f}")
        
        # Detect and highlight anomalies (top 3 only)
        anomalies = self._detect_anomalies(df, limit=3)
        if anomalies:
            sections.append("\n**Key Anomalies (Focus Areas):**")
            for anomaly in anomalies:
                sections.append(f"- {anomaly}")
        
        # Top/bottom performers (limit to top 3 each)
        top_performers = self._get_top_performers(df, n=3)
        if top_performers:
            sections.append("\n**Top Performers:**")
            for performer in top_performers:
                sections.append(f"- {performer}")
        
        bottom_performers = self._get_bottom_performers(df, n=3)
        if bottom_performers:
            sections.append("\n**Bottom Performers (Concerns):**")
            for performer in bottom_performers:
                sections.append(f"- {performer}")
        
        # Full dataset (limited rows for token optimization)
        sections.append(f"\n**Full Dataset (showing first {min(max_rows, len(df))} of {len(df)} rows):**")
        sections.append(df.head(max_rows).to_string(index=False))
        
        if len(df) > max_rows:
            sections.append(f"\n[... {len(df) - max_rows} more rows not shown for token optimization ...]")
        
        return "\n".join(sections)
    
    def _detect_anomalies(self, df: pd.DataFrame, limit: int = 3) -> List[str]:
        """
        Detect statistical anomalies in numeric columns (token-optimized, top N only).
        
        Args:
            df: DataFrame to analyze
            limit: Maximum number of anomalies to return
        """
        if not PANDAS_AVAILABLE or len(df) < 5:
            return []
        
        anomalies = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
            values = df[col].dropna()
            if len(values) < 3:
                continue
            
            if SCIPY_AVAILABLE:
                # Use z-score method
                z_scores = np.abs(stats.zscore(values))
                threshold = 2.5
                anomaly_indices = np.where(z_scores > threshold)[0]
            else:
                # Simple IQR method
                Q1 = values.quantile(0.25)
                Q3 = values.quantile(0.75)
                IQR = Q3 - Q1
                threshold = Q3 + 1.5 * IQR
                anomaly_indices = values[values > threshold].index
            
            if len(anomaly_indices) > 0:
                # Get the most extreme anomaly
                idx = anomaly_indices[0]
                row = df.iloc[idx]
                anomaly_val = row[col]
                avg_val = values.mean()
                pct_change = ((anomaly_val - avg_val) / avg_val * 100) if avg_val != 0 else 0
                
                # Format anomaly description
                date_col = [c for c in df.columns if 'date' in c.lower()]
                if date_col:
                    date_val = row[date_col[0]]
                    anomalies.append(f"{date_val}: {col} = {anomaly_val:.2f} ({pct_change:+.1f}% vs avg {avg_val:.2f})")
                else:
                    anomalies.append(f"Row {idx}: {col} = {anomaly_val:.2f} ({pct_change:+.1f}% vs avg {avg_val:.2f})")
                
                if len(anomalies) >= limit:
                    break
        
        return anomalies
    
    def _get_top_performers(self, df: pd.DataFrame, n: int = 3) -> List[str]:
        """
        Get top N performers (token-optimized).
        
        Args:
            df: DataFrame to analyze
            n: Number of top performers to return
        """
        if not PANDAS_AVAILABLE or len(df) == 0:
            return []
        
        # Try to find a primary metric column (revenue, retention, etc.)
        # Only consider numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) == 0:
            return []
        
        primary_metrics = ['revenue', 'retention', 'arpu', 'dau', 'conversion']
        metric_col = None
        
        for metric in primary_metrics:
            matching_cols = [col for col in numeric_cols if metric in col.lower()]
            if matching_cols:
                metric_col = matching_cols[0]
                break
        
        if not metric_col:
            # Fallback to first numeric column
            metric_col = numeric_cols[0]
        
        # Get top N by the metric
        top_n = df.nlargest(n, metric_col)
        
        performers = []
        date_col = [c for c in df.columns if 'date' in c.lower()] or [c for c in df.columns if 'cohort' in c.lower()]
        
        for idx, row in top_n.iterrows():
            metric_val = row[metric_col]
            
            # Try to get identifier (date, cohort, region, etc.)
            identifier = None
            if date_col:
                identifier = str(row[date_col[0]])
            else:
                # Try other common identifier columns
                id_cols = [c for c in df.columns if any(x in c.lower() for x in ['id', 'name', 'segment', 'region', 'country'])]
                if id_cols:
                    identifier = str(row[id_cols[0]])
            
            if identifier:
                performers.append(f"{identifier}: {metric_col}={metric_val:.2f}")
            else:
                performers.append(f"Row {idx}: {metric_col}={metric_val:.2f}")
        
        return performers
    
    def _get_bottom_performers(self, df: pd.DataFrame, n: int = 3) -> List[str]:
        """
        Get bottom N performers (token-optimized).
        
        Args:
            df: DataFrame to analyze
            n: Number of bottom performers to return
        """
        if not PANDAS_AVAILABLE or len(df) == 0:
            return []
        
        # Try to find a primary metric column
        # Only consider numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) == 0:
            return []
        
        primary_metrics = ['revenue', 'retention', 'arpu', 'dau', 'conversion']
        metric_col = None
        
        for metric in primary_metrics:
            matching_cols = [col for col in numeric_cols if metric in col.lower()]
            if matching_cols:
                metric_col = matching_cols[0]
                break
        
        if not metric_col:
            # Fallback to first numeric column
            metric_col = numeric_cols[0]
        
        # Get bottom N by the metric
        bottom_n = df.nsmallest(n, metric_col)
        
        performers = []
        date_col = [c for c in df.columns if 'date' in c.lower()] or [c for c in df.columns if 'cohort' in c.lower()]
        
        for idx, row in bottom_n.iterrows():
            metric_val = row[metric_col]
            
            identifier = None
            if date_col:
                identifier = str(row[date_col[0]])
            else:
                id_cols = [c for c in df.columns if any(x in c.lower() for x in ['id', 'name', 'segment', 'region', 'country'])]
                if id_cols:
                    identifier = str(row[id_cols[0]])
            
            if identifier:
                performers.append(f"{identifier}: {metric_col}={metric_val:.2f}")
            else:
                performers.append(f"Row {idx}: {metric_col}={metric_val:.2f}")
        
        return performers
    
    def get_few_shot_examples(self) -> str:
        """
        Load few-shot examples from file.
        Returns empty string if file doesn't exist (for manual addition later).
        """
        if not self.example_file_path.exists():
            return ""  # File will be created later with examples
        
        try:
            with open(self.example_file_path, 'r') as f:
                content = f.read().strip()
            return content if content else ""
        except Exception:
            return ""
    
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
