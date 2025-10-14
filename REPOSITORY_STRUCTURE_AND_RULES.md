# Repository Structure & Key Rules Summary

## Project Overview
**Purpose**: Automated data analysis and due diligence system  
**Core Function**: Hypothesis-driven analysis with statistical rigor and memory accumulation  
**Primary Command**: `/analysis-workflow` - runs the comprehensive analysis workflow

## Repository Structure

```
project-root/
├── context/                    # Domain knowledge and specifications
│   ├── final/                 # Core system architecture specs
│   └── generated/             # Auto-generated documentation
├── workflows/                 # Analysis workflows and procedures
├── scripts/                   # Analysis and utility scripts
│   └── schema_exploration/    # Automated schema discovery tools
├── archive/                   # Historical analysis results
│   ├── old_outputs/reports/   # Previous analysis reports
│   └── old_scripts/           # Deprecated analysis scripts
├── debug/                     # Error logs and improvement tracking
├── run_logs/                  # Dynamic run outputs (created during execution)
│   └── {run_hash}/            # Individual analysis run folders
└── .env                       # Environment configuration
```

## Key Rules & Patterns

### 1. **Run-Based Organization**
- **Every analysis gets unique 6-character hash**: `export RUN_HASH=$(python3 -c "import secrets; print(secrets.token_hex(3))")`
- **All outputs go to run-specific folders**: `run_logs/${RUN_HASH}/outputs/`
- **Working scripts in run folders**: `run_logs/${RUN_HASH}/working/`
- **Never create files in repository root** during analysis

### 2. **Environment Variable Management**
```bash
# Project-level .env file
DATASET_NAME=your_dataset.table_name
API_KEY=your_api_key_here
EVENT_NAME_COLUMN=event_name
USER_ID_COLUMN=user_id
DEVICE_ID_COLUMN=device_id
TIMESTAMP_COLUMN=timestamp
REVENUE_COLUMN=revenue
ANALYSIS_WINDOW_DAYS=90

# Run-specific .env file created for each analysis
echo "export RUN_HASH=${RUN_HASH}" > run_logs/${RUN_HASH}/.env
```

### 3. **Script Execution Pattern**
```python
# All scripts must use environment variables
import os
RUN_HASH = os.environ.get('RUN_HASH')
DATASET_NAME = os.environ.get('DATASET_NAME')
OUTPUTS_DIR = f'run_logs/{RUN_HASH}/outputs'
WORKING_DIR = f'run_logs/{RUN_HASH}/working'

# Use absolute paths when creating files
script_path = os.path.join(os.getcwd(), WORKING_DIR, 'script_name.py')
```

```bash
# Always source environment before running scripts
source run_logs/${RUN_HASH}/.env && python3 run_logs/${RUN_HASH}/working/script_name.py
```

### 4. **Output Structure Standard**
```
run_logs/{run_hash}/
├── outputs/
│   ├── schema/          # Data dictionary and mappings
│   ├── segments/        # User segment definitions and statistics
│   ├── iterations/      # Hypothesis test results
│   ├── memory/          # Accumulated learnings
│   └── run_summary.md   # Overview and index
├── working/             # Scripts and queries used
└── .env                 # Run-specific environment
```

### 5. **Statistical Rigor Requirements**
- **Minimum sample size**: n≥30 for all statistical tests
- **Significance threshold**: p<0.05 for validated findings
- **Confidence intervals**: Required for all key metrics
- **Segment grounding**: All analysis must start with time-based segments
- **Effect size reporting**: Quantify practical significance

### 6. **Memory & Learning System**
```json
{
  "findings": [],
  "failed_hypotheses": [],
  "successful_patterns": [],
  "segment_insights": {},
  "confidence_scores": {}
}
```
- **Avoid repeating failed approaches** stored in memory
- **Build cumulative knowledge** across iterations
- **Store validated findings** with confidence scores
- **Track successful patterns** for reuse

### 7. **Cost Management**
- **Dry-run first**: Estimate query costs before execution
- **Default time window**: Balance data freshness with cost
- **Query optimization**: Use aggregations, avoid raw data pulls
- **Budget tracking**: Monitor costs per run and iteration

### 8. **Error Handling & Recovery**
- **Graceful degradation**: Continue with partial results if possible
- **Error logging**: Save errors to `run_logs/${RUN_HASH}/outputs/errors.log`
- **Partial result preservation**: Don't lose completed work
- **Recovery suggestions**: Provide next steps in error reports

### 9. **Quality Assurance**
- **Schema validation**: Complete mapping before analysis
- **Data quality checks**: Null rates, gaps, duplicates
- **Metric validation**: Cross-check different calculation methods
- **User identification**: Handle anonymous vs identified users properly

### 10. **Documentation Standards**
- **Comprehensive comments**: Explain business logic and statistical methods
- **Data dictionaries**: Document all columns and event types
- **Methodology documentation**: Explain analysis approach
- **Confidence scoring**: Rate reliability of all findings

## Implementation Checklist

### For New Analysis Runs:
- [ ] Generate unique run hash
- [ ] Create run directory structure
- [ ] Set up environment variables
- [ ] Complete schema discovery
- [ ] Define segments with time-based grounding
- [ ] Generate and prioritize hypotheses
- [ ] Execute iterative testing with memory accumulation
- [ ] Create comprehensive run summary

### For Script Development:
- [ ] Use environment variables for all paths
- [ ] Include statistical validation
- [ ] Add dry-run cost estimation
- [ ] Implement error handling
- [ ] Update memory system
- [ ] Document methodology
- [ ] Save outputs to correct run folder

### For Output Review:
- [ ] Check run_summary.md first
- [ ] Verify statistical significance
- [ ] Review confidence scores
- [ ] Examine data quality notes
- [ ] Consider follow-up recommendations

This structure ensures **reproducible, traceable, and statistically rigorous analysis** with proper cost control and quality assurance.

## Key Principles

### **Modularity**
- Each phase is self-contained with clear inputs and outputs
- Scripts are reusable across different analysis runs
- Memory system allows learning accumulation across projects

### **Reproducibility**
- Unique run hashes ensure no conflicts between analyses
- Environment variables provide consistent configuration
- All outputs are versioned and traceable

### **Scalability**
- Run-based organization supports parallel analysis
- Memory system prevents redundant work
- Cost management enables large-scale analysis

### **Quality**
- Statistical rigor ensures reliable findings
- Error handling maintains system stability
- Documentation standards support knowledge transfer
