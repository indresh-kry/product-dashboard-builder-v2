# LLM Child Revenue Optimization Analyst v1.0.0

## Overview
Specialized LLM for analyzing revenue streams and monetization opportunities. Part of the multi-LLM architecture, focusing specifically on revenue optimization and monetization strategies.

## Functions

### `call_openai_api(prompt: str) -> Dict`
- **Purpose**: Calls OpenAI API to generate revenue optimization insights
- **Input**: Revenue optimization analysis prompt
- **Output**: Parsed JSON response or error structure
- **Tools**: OpenAI GPT-4 API
- **Variables**: 
  - Input: `prompt` (function parameter)
  - Environment: `OPENAI_API_KEY` (from environment)

### `load_revenue_data(run_hash: str) -> Dict`
- **Purpose**: Loads revenue optimization data from Phase 3 outputs
- **Input**: Run hash for data location
- **Output**: Dictionary containing revenue performance data
- **Tools**: Pandas for CSV reading, file system operations
- **Variables**:
  - Input: `run_hash` (function parameter)
  - File paths: `revenue_segments_daily.csv`, `revenue_by_type.csv`, `revenue_by_cohort_date.csv`
  - Computed: `segment_distribution`

### `generate_revenue_prompt(data: Dict, run_metadata: Dict) -> str`
- **Purpose**: Generates the revenue optimization analysis prompt
- **Input**: Revenue data and run metadata
- **Output**: Formatted prompt string
- **Tools**: String formatting, JSON serialization
- **Variables**:
  - Input: `data`, `run_metadata` (function parameters)
  - Computed: Formatted prompt with data snippets

### `analyze_revenue_optimization(run_hash: str, run_metadata: Dict) -> Dict`
- **Purpose**: Main analysis function for revenue optimization
- **Input**: Run hash and metadata
- **Output**: Revenue optimization insights with metadata
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
- **OpenAI API**: GPT-4 model for revenue optimization analysis
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
- `revenue_segments_daily.csv`: Revenue performance by user segments
- `revenue_by_type.csv`: Revenue breakdown by type (IAP, ads, subscription)
- `revenue_by_cohort_date.csv`: Revenue performance by cohort date

### Hardcoded Variables
- Model: "gpt-4"
- Temperature: 0.3
- Max tokens: 1000
- System prompt: Specialized revenue optimization analyst role
- Data file paths: `run_logs/{run_hash}/outputs/segments/user_level/` and `run_logs/{run_hash}/outputs/segments/daily/`

### Computed Variables
- `segment_distribution`: Revenue segment distribution
- `insights`: Final analysis results
- `metadata`: Execution metadata (timestamp, run hash, analyst type)

## Function Call Flow

```mermaid
graph TD
    A[analyze_revenue_optimization] --> B[load_revenue_data]
    B --> C[generate_revenue_prompt]
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
    K --> L[Return Revenue Optimization Insights]
```

## Key Features
- **Focused Analysis**: Specialized in revenue streams and monetization patterns
- **Data Integration**: Loads data from Phase 3 segmentation outputs
- **Robust JSON Parsing**: Multiple fallback methods for response parsing
- **Revenue Analysis**: Analyzes different revenue streams and performance
- **Monetization Insights**: Provides monetization optimization recommendations

## Analysis Focus Areas
- **Revenue Stream Performance**: Analysis of different revenue streams
- **User Monetization Patterns**: How users engage with monetization
- **Pricing Optimization**: Opportunities for pricing improvements
- **Revenue Growth Strategies**: Strategies to increase revenue

## Dependencies
- `openai`: OpenAI API client
- `pandas`: Data manipulation and analysis
- `json`: JSON serialization
- `os`: Environment variable access
- `datetime`: Timestamp generation
- `re`: Regular expressions for JSON extraction

## Version History
- **v1.0.0** (2025-10-16): Initial version with revenue optimization analysis capabilities
