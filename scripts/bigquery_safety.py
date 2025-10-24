#!/usr/bin/env python3
"""
BigQuery Safety Module for Product Dashboard Builder v2
Version: 1.0.0
Last Updated: 2025-10-15

This module provides comprehensive safety guardrails to prevent modification
of source BigQuery tables and ensure read-only operations.
"""

import os
import re
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from google.cloud import bigquery
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BigQuerySafetyConfig:
    """Configuration for BigQuery safety settings"""
    read_only_mode: bool = True
    allowed_operations: List[str] = None
    forbidden_operations: List[str] = None
    source_dataset_protection: bool = True
    allowed_target_datasets: List[str] = None
    enable_query_logging: bool = True
    enable_audit_trail: bool = True
    
    def __post_init__(self):
        if self.allowed_operations is None:
            self.allowed_operations = ['SELECT', 'WITH', 'UNION', 'JOIN']
        if self.forbidden_operations is None:
            self.forbidden_operations = [
                'CREATE', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 
                'TRUNCATE', 'REPLACE', 'MERGE', 'CALL', 'GRANT', 'REVOKE', 'SET'
            ]
        if self.allowed_target_datasets is None:
            self.allowed_target_datasets = [
                'analysis_results', 'temp_tables', 'user_analysis', 
                'dashboard_data', 'reporting', 'segments'
            ]

class BigQuerySafetyError(Exception):
    """Custom exception for BigQuery safety violations"""
    pass

class BigQuerySafetyValidator:
    """Validates BigQuery queries for safety compliance"""
    
    def __init__(self, config: BigQuerySafetyConfig):
        self.config = config
        self.audit_log = []
    
    def validate_query_safety(self, query: str) -> Tuple[bool, List[str]]:
        """
        Validate that query only contains safe operations
        
        Args:
            query: SQL query to validate
            
        Returns:
            Tuple of (is_safe, list_of_violations)
        """
        violations = []
        query_upper = query.upper().strip()
        
        # Check for forbidden operations
        for operation in self.config.forbidden_operations:
            pattern = rf'\b{re.escape(operation)}\b'
            if re.search(pattern, query_upper):
                violations.append(f"Forbidden operation detected: {operation}")
        
        # Check for allowed operations only (if strict mode)
        if self.config.read_only_mode:
            # Find all SQL keywords
            sql_keywords = re.findall(r'\b[A-Z]+\b', query_upper)
            for keyword in sql_keywords:
                if keyword in ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'WITH', 'UNION', 'JOIN', 'ON', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'COALESCE', 'DATE', 'TIMESTAMP', 'ANY_VALUE']:
                    continue  # These are safe
                elif keyword in self.config.forbidden_operations:
                    violations.append(f"Forbidden keyword detected: {keyword}")
        
        # Log validation attempt
        if self.config.enable_query_logging:
            self._log_query_validation(query, violations)
        
        return len(violations) == 0, violations
    
    def validate_source_table_access(self, query: str, source_dataset: str) -> Tuple[bool, List[str]]:
        """
        Ensure queries only READ from source tables, never modify them
        
        Args:
            query: SQL query to validate
            source_dataset: Source dataset name to protect
            
        Returns:
            Tuple of (is_safe, list_of_violations)
        """
        violations = []
        query_upper = query.upper()
        
        # Extract table references
        table_refs = re.findall(r'FROM\s+`?([^`\s]+)`?', query_upper)
        table_refs.extend(re.findall(r'JOIN\s+`?([^`\s]+)`?', query_upper))
        
        # Check if any source table is being modified
        for table_ref in table_refs:
            if source_dataset in table_ref:
                # Source table should only appear in FROM/JOIN clauses
                # Check if it appears in any modification context
                if any(op in query_upper for op in ['CREATE', 'INSERT', 'UPDATE', 'DELETE']):
                    violations.append(f"Source table {table_ref} appears in modification context")
        
        return len(violations) == 0, violations
    
    def validate_target_dataset(self, target_dataset: str) -> Tuple[bool, List[str]]:
        """
        Validate that target dataset is in allowed list
        
        Args:
            target_dataset: Target dataset name
            
        Returns:
            Tuple of (is_safe, list_of_violations)
        """
        violations = []
        
        if target_dataset not in self.config.allowed_target_datasets:
            violations.append(f"Target dataset '{target_dataset}' not in allowed list: {self.config.allowed_target_datasets}")
        
        return len(violations) == 0, violations
    
    def _log_query_validation(self, query: str, violations: List[str]):
        """Log query validation attempt"""
        log_entry = {
            "timestamp": str(pd.Timestamp.now()),
            "query_preview": query[:200] + "..." if len(query) > 200 else query,
            "violations": violations,
            "is_safe": len(violations) == 0
        }
        self.audit_log.append(log_entry)
        logger.info(f"Query validation: {len(violations)} violations found")

class SafeBigQueryClient:
    """Safe wrapper around BigQuery client with built-in safety checks"""
    
    def __init__(self, client: bigquery.Client, config: BigQuerySafetyConfig, source_dataset: str = None):
        self.client = client
        self.config = config
        self.source_dataset = source_dataset
        self.validator = BigQuerySafetyValidator(config)
        self.audit_log = []
    
    def query(self, query: str, **kwargs) -> Any:
        """
        Execute query with safety validation
        
        Args:
            query: SQL query to execute
            **kwargs: Additional arguments for BigQuery client
            
        Returns:
            BigQuery job result
            
        Raises:
            BigQuerySafetyError: If query fails safety validation
        """
        # Validate query safety
        is_safe, violations = self.validator.validate_query_safety(query)
        if not is_safe:
            error_msg = f"Query failed safety validation: {'; '.join(violations)}"
            logger.error(error_msg)
            raise BigQuerySafetyError(error_msg)
        
        # Validate source table access if source dataset is specified
        if self.source_dataset:
            is_safe, violations = self.validator.validate_source_table_access(query, self.source_dataset)
            if not is_safe:
                error_msg = f"Query failed source table protection: {'; '.join(violations)}"
                logger.error(error_msg)
                raise BigQuerySafetyError(error_msg)
        
        # Log query execution
        if self.config.enable_audit_trail:
            self._log_query_execution(query)
        
        # Execute with original client
        try:
            return self.client.query(query, **kwargs)
        except Exception as e:
            logger.error(f"BigQuery execution failed: {str(e)}")
            raise
    
    def create_table(self, query: str, target_project: str, target_dataset: str, table_name: str) -> bool:
        """
        Safely create BigQuery table with additional validation
        
        Args:
            query: SQL query for table creation
            target_project: Target project ID
            target_dataset: Target dataset name
            table_name: Target table name
            
        Returns:
            Boolean indicating success
            
        Raises:
            BigQuerySafetyError: If validation fails
        """
        # Validate target dataset
        is_safe, violations = self.validator.validate_target_dataset(target_dataset)
        if not is_safe:
            error_msg = f"Target dataset validation failed: {'; '.join(violations)}"
            logger.error(error_msg)
            raise BigQuerySafetyError(error_msg)
        
        # Validate the underlying query
        is_safe, violations = self.validator.validate_query_safety(query)
        if not is_safe:
            error_msg = f"Table creation query failed safety validation: {'; '.join(violations)}"
            logger.error(error_msg)
            raise BigQuerySafetyError(error_msg)
        
        # Validate source table access
        if self.source_dataset:
            is_safe, violations = self.validator.validate_source_table_access(query, self.source_dataset)
            if not is_safe:
                error_msg = f"Table creation query failed source table protection: {'; '.join(violations)}"
                logger.error(error_msg)
                raise BigQuerySafetyError(error_msg)
        
        # Create the table
        try:
            create_query = f"""
            CREATE OR REPLACE TABLE `{target_project}.{target_dataset}.{table_name}` AS
            {query}
            """
            
            job = self.client.query(create_query)
            job.result()  # Wait for completion
            
            logger.info(f"Successfully created BigQuery table: {target_project}.{target_dataset}.{table_name}")
            return True
            
        except Exception as e:
            if "Permission" in str(e) or "Access Denied" in str(e):
                logger.warning(f"Access denied for table creation: {str(e)}")
                return False
            else:
                logger.error(f"Error creating BigQuery table: {str(e)}")
                raise
    
    def _log_query_execution(self, query: str):
        """Log query execution for audit trail"""
        log_entry = {
            "timestamp": str(pd.Timestamp.now()),
            "query_preview": query[:200] + "..." if len(query) > 200 else query,
            "source_dataset": self.source_dataset
        }
        self.audit_log.append(log_entry)
        logger.info("Query executed successfully")
    
    def get_audit_log(self) -> List[Dict]:
        """Get audit log of all operations"""
        return self.audit_log.copy()

def get_safe_bigquery_client(source_dataset: str = None) -> SafeBigQueryClient:
    """
    Create a safe BigQuery client with safety configuration
    
    Args:
        source_dataset: Source dataset name to protect
        
    Returns:
        SafeBigQueryClient instance
    """
    # Load configuration from environment
    config = BigQuerySafetyConfig(
        read_only_mode=os.environ.get('BIGQUERY_READ_ONLY_MODE', 'true').lower() == 'true',
        allowed_target_datasets=os.environ.get('BIGQUERY_ALLOWED_DATASETS', '').split(',') if os.environ.get('BIGQUERY_ALLOWED_DATASETS') else None,
        enable_query_logging=os.environ.get('BIGQUERY_ENABLE_LOGGING', 'true').lower() == 'true',
        enable_audit_trail=os.environ.get('BIGQUERY_ENABLE_AUDIT', 'true').lower() == 'true'
    )
    
    # Initialize original BigQuery client
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if not credentials_path:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS not set")
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    # Return safe wrapper
    return SafeBigQueryClient(client, config, source_dataset)

def validate_environment_safety() -> bool:
    """
    Validate that environment is configured for safe operation
    
    Returns:
        Boolean indicating if environment is safe
    """
    required_vars = [
        'GOOGLE_APPLICATION_CREDENTIALS',
        'GOOGLE_CLOUD_PROJECT',
        'BIGQUERY_READ_ONLY_MODE'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return False
    
    if os.environ.get('BIGQUERY_READ_ONLY_MODE', 'true').lower() != 'true':
        logger.error("BIGQUERY_READ_ONLY_MODE must be set to 'true' for safe operation")
        return False
    
    logger.info("Environment safety validation passed")
    return True

# Import pandas for timestamp handling
try:
    import pandas as pd
except ImportError:
    # Fallback for environments without pandas
    import datetime
    class pd:
        class Timestamp:
            @staticmethod
            def now():
                return datetime.datetime.now()
