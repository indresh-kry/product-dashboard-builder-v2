# LLM Child Geographic Analyst v1.0.0

## Overview
Specialized LLM for analyzing performance by country and region. Part of the multi-LLM architecture, focusing specifically on geographic performance patterns and market analysis.

## Functions

### `call_openai_api(prompt: str) -> Dict`
- **Purpose**: Calls OpenAI API to generate geographic insights
- **Input**: Geographic analysis prompt
- **Output**: Parsed JSON response or error structure
- **Tools**: OpenAI GPT-4 API
- **Variables**: 
  - Input: `prompt` (function parameter)
  - Environment: `OPENAI_API_KEY` (from environment)

### `load_geographic_data(run_hash: str) -> Dict`
- **Purpose**: Loads geographic data from Phase 3 outputs
- **Input**: Run hash for data location
- **Output**: Dictionary containing country-level performance data
- **Tools**: Pandas for CSV reading, file system operations
- **Variables**:
  - Input: `run_hash` (function parameter)
  - File paths: `dau_by_country.csv`, `revenue_by_country.csv`, `new_logins_by_country.csv`
  - Computed: `country_distribution`

### `generate_geographic_prompt(data: Dict, run_metadata: Dict) -> str`
- **Purpose**: Generates the geographic analysis prompt
- **Input**: Geographic data and run metadata
- **Output**: Formatted prompt string
- **Tools**: String formatting, JSON serialization
- **Variables**:
  - Input: `data`, `run_metadata` (function parameters)
  - Computed: Formatted prompt with data snippets

### `analyze_geographic(run_hash: str, run_metadata: Dict) -> Dict`
- **Purpose**: Main analysis function for geographic performance
- **Input**: Run hash and metadata
- **Output**: Geographic insights with metadata
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
- **OpenAI API**: GPT-4 model for geographic analysis
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
- `dau_by_country.csv`: Daily active users by country
- `revenue_by_country.csv`: Revenue performance by country
- `new_logins_by_country.csv`: New user acquisition by country

### Hardcoded Variables
- Model: "gpt-4"
- Temperature: 0.3
- Max tokens: 1000
- System prompt: Specialized geographic analyst role
- Data file paths: `run_logs/{run_hash}/outputs/segments/daily/`

### Computed Variables
- `country_distribution`: Distribution of users across countries
- `insights`: Final analysis results
- `metadata`: Execution metadata (timestamp, run hash, analyst type)

## Function Call Flow

```mermaid
graph TD
    A[analyze_geographic] --> B[load_geographic_data]
    B --> C[generate_geographic_prompt]
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
    K --> L[Return Geographic Insights]
```

## Key Features
- **Focused Analysis**: Specialized in geographic performance patterns
- **Data Integration**: Loads data from Phase 3 segmentation outputs
- **Robust JSON Parsing**: Multiple fallback methods for response parsing
- **Market Analysis**: Identifies primary and secondary markets
- **Localization Insights**: Provides localization recommendations

## Analysis Focus Areas
- **Market Concentration**: Primary and secondary markets
- **Regional Performance**: Performance differences by region
- **Localization Opportunities**: Areas needing localization
- **Market Expansion**: Potential for new market entry

## Dependencies
- `openai`: OpenAI API client
- `pandas`: Data manipulation and analysis
- `json`: JSON serialization
- `os`: Environment variable access
- `datetime`: Timestamp generation
- `re`: Regular expressions for JSON extraction

## Version History
- **v1.0.0** (2025-10-16): Initial version with geographic analysis capabilities
