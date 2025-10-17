# LLM Child Cohort & Retention Analyst v1.0.0

## Overview
Specialized LLM for analyzing user retention and cohort performance. Part of the multi-LLM architecture, focusing specifically on cohort analysis and retention optimization.

## Functions

### `call_openai_api(prompt: str) -> Dict`
- **Purpose**: Calls OpenAI API to generate cohort and retention insights
- **Input**: Cohort and retention analysis prompt
- **Output**: Parsed JSON response or error structure
- **Tools**: OpenAI GPT-4 API
- **Variables**: 
  - Input: `prompt` (function parameter)
  - Environment: `OPENAI_API_KEY` (from environment)

### `load_cohort_data(run_hash: str) -> Dict`
- **Purpose**: Loads cohort and retention data from Phase 3 outputs
- **Input**: Run hash for data location
- **Output**: Dictionary containing cohort performance data
- **Tools**: Pandas for CSV reading, file system operations
- **Variables**:
  - Input: `run_hash` (function parameter)
  - File paths: `dau_by_cohort_date.csv`, `engagement_by_cohort_date.csv`, `revenue_by_cohort_date.csv`
  - Computed: `cohort_summary`

### `generate_cohort_prompt(data: Dict, run_metadata: Dict) -> str`
- **Purpose**: Generates the cohort and retention analysis prompt
- **Input**: Cohort data and run metadata
- **Output**: Formatted prompt string
- **Tools**: String formatting, JSON serialization
- **Variables**:
  - Input: `data`, `run_metadata` (function parameters)
  - Computed: Formatted prompt with data snippets

### `analyze_cohort_retention(run_hash: str, run_metadata: Dict) -> Dict`
- **Purpose**: Main analysis function for cohort and retention
- **Input**: Run hash and metadata
- **Output**: Cohort and retention insights with metadata
- **Tools**: Data loading, prompt generation, LLM API
- **Variables**:
  - Input: `run_hash`, `run_metadata` (function parameters)
  - Computed: `insights`, `metadata`

### `main()`
- **Purpose**: Test function for standalone execution
- **Input**: None (uses environment variables)
- **Output**: Test results
- **Tools**: Environment variable access, JSON serialization
- **Variables**:
  - Environment: `RUN_HASH` (from environment)
  - Hardcoded: Test run metadata

## Tools Used
- **OpenAI API**: GPT-4 model for cohort and retention analysis
- **Pandas**: Data manipulation and CSV reading
- **JSON**: Serialization and parsing
- **Regular Expressions**: JSON extraction from markdown
- **Datetime**: Timestamp generation

## Variables by Source

### Input Variables
- `run_hash`: Unique identifier for the current run
- `run_metadata`: Dictionary with run information (date range, data source)

### Environment Variables
- `OPENAI_API_KEY`: OpenAI API key for LLM access
- `RUN_HASH`: Current run identifier (for testing)

### File Input Variables
- `dau_by_cohort_date.csv`: Daily active users by cohort date
- `engagement_by_cohort_date.csv`: Engagement metrics by cohort date
- `revenue_by_cohort_date.csv`: Revenue performance by cohort date

### Hardcoded Variables
- Model: "gpt-4"
- Temperature: 0.3
- Max tokens: 1000
- System prompt: Specialized cohort and retention analyst role
- Data file paths: `run_logs/{run_hash}/outputs/segments/cohort/`

### Computed Variables
- `cohort_summary`: Summary statistics from cohort data
- `insights`: Final analysis results
- `metadata`: Execution metadata (timestamp, run hash, analyst type)

## Function Call Flow

```mermaid
graph TD
    A[analyze_cohort_retention] --> B[load_cohort_data]
    B --> C[generate_cohort_prompt]
    C --> D[call_openai_api]
    D --> E{JSON Parse Success?}
    E -->|Yes| F[Return Parsed JSON]
    E -->|No| G[Extract from Markdown]
    G --> H{Extraction Success?}
    H -->|Yes| I[Return Extracted JSON]
    H -->|No| J[Return Error Structure]
    F --> K[Add Metadata]
    I --> K
    J --> K
    K --> L[Return Cohort & Retention Insights]
```

## Key Features
- **Focused Analysis**: Specialized in cohort performance and retention patterns
- **Data Integration**: Loads data from Phase 3 segmentation outputs
- **Robust JSON Parsing**: Multiple fallback methods for response parsing
- **Retention Analysis**: Analyzes retention curves and key milestones
- **Churn Prevention**: Identifies churn patterns and risk factors

## Analysis Focus Areas
- **Retention Patterns**: Retention curve analysis and key milestones
- **Cohort Performance**: Comparison of different cohort performance
- **Churn Analysis**: Churn patterns and risk factors
- **Retention Strategies**: Recommendations for improving retention

## Dependencies
- `openai`: OpenAI API client
- `pandas`: Data manipulation and analysis
- `json`: JSON serialization
- `os`: Environment variable access
- `datetime`: Timestamp generation
- `re`: Regular expressions for JSON extraction

## Version History
- **v1.0.0** (2025-10-16): Initial version with cohort and retention analysis capabilities
