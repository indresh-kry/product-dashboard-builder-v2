# Product Dashboard Builder v2

Automated data analysis and due diligence system for clickstream data with AI-powered insights.

## 🚀 Quick Start

This system provides end-to-end automated analysis of clickstream data, from schema discovery through AI-powered insights generation. The workflow is orchestrated through a series of Python scripts that work together to deliver comprehensive product metrics and actionable insights.

### Prerequisites

- Python 3.9+
- Google Cloud credentials for BigQuery access
- OpenAI API key for LLM insights generation
- Required Python packages (see dependencies below)

### 🔑 API Key Setup

**OpenAI API Key Configuration:**
1. Add your OpenAI API key to `creds.json`:
   ```json
   {
     "openai_api_key": "sk-your-actual-openai-api-key-here"
   }
   ```
2. The system will automatically load the key for Phase 5 (LLM Insights Generation)

**Security Note:** The `creds.json` file is in `.gitignore` to prevent accidental commits of sensitive credentials.

### Basic Usage

```bash
# Run complete analysis workflow
python scripts/analysis_workflow_orchestrator.py

# Run with specific app and date filters
python scripts/analysis_workflow_orchestrator.py --app-filter com.nukebox.mandir --date-start 2025-09-01 --date-end 2025-09-07
```

## 📚 Understanding the Code

**New to the codebase?** Start here to understand how the system works:

### 🔍 [Script Documentation](./scripts/)

The `scripts/` directory contains comprehensive documentation for all active scripts, including:

- **Function overviews** with parameters and descriptions
- **External tools and APIs** used by each script
- **Variables & Configuration** organized by source (input, env, hardcoded, computed)
- **Mermaid flow diagrams** showing function call sequences

### 🧠 Multi-LLM Architecture

The system uses a sophisticated multi-LLM architecture for generating insights:

- **6 Specialized Child LLMs**: Each focused on a specific domain (daily metrics, user segmentation, geographic analysis, cohort retention, revenue optimization, data quality)
- **1 Parent Coordinator LLM**: Synthesizes insights from all child LLMs into executive recommendations
- **Parallel Processing**: Child LLMs run simultaneously for faster execution
- **Token Optimization**: Concise prompts to stay within API limits
#### Available Script Documentation:

**Core Workflow Scripts:**
- [Analysis Workflow Orchestrator](./scripts/analysis_workflow_orchestrator.md) - Central orchestrator for the entire workflow
- [System Health Check](./scripts/system_health_check.md) - Environment validation and health checks
- [Schema Discovery v3](./scripts/schema_discovery_v3.md) - Enhanced schema discovery with session and revenue analysis
- [Data Aggregation v3](./scripts/data_aggregation_v3.md) - User-level data aggregation with safety guards
- [User Segmentation v1](./scripts/user_segmentation_v1.md) - User segmentation and behavioral analysis

**Multi-LLM Architecture Scripts:**
- [Multi-LLM Insights Orchestrator](./scripts/llm_insights_multi_v1.md) - Main orchestrator for multi-LLM system
- [LLM Coordinator](./scripts/llm_coordinator_v1.md) - Parent LLM for synthesizing child insights
- [Daily Metrics Analyst](./scripts/llm_child_daily_metrics_v1.md) - Specialized daily metrics analysis
- [User Segmentation Analyst](./scripts/llm_child_user_segmentation_v1.md) - Specialized user segmentation analysis
- [Geographic Analyst](./scripts/llm_child_geographic_v1.md) - Specialized geographic performance analysis
- [Cohort & Retention Analyst](./scripts/llm_child_cohort_retention_v1.md) - Specialized cohort and retention analysis
- [Revenue Optimization Analyst](./scripts/llm_child_revenue_optimization_v1.md) - Specialized revenue optimization analysis
- [Data Quality Analyst](./scripts/llm_child_data_quality_v1.md) - Specialized data quality analysis

**Supporting Scripts:**
- [Rules Engine Integration](./scripts/rules_engine_integration.md) - Advanced rule processing capabilities
- [BigQuery Safety](./scripts/bigquery_safety.md) - Safety guardrails for BigQuery operations

### 📋 [Workflow Guidelines](./workflow_guidelines/)

The `workflow_guidelines/` directory contains the foundational documentation:

- **[Repository Structure and Rules](./workflow_guidelines/REPOSITORY_STRUCTURE_AND_RULES.md)** - Development standards and patterns
- **[High-Level Design](./workflow_guidelines/high-level-design.md)** - System architecture and design principles
- **[Sequence Diagram](./workflow_guidelines/sequenceDiagram.md)** - Core orchestration flow visualization
- **[Data Aggregation Table Structure](./workflow_guidelines/data_aggregation_table_structure.md)** - Primary data structure specifications

## 🏗️ System Architecture

The system follows a 6-phase workflow:

1. **Phase 0: System Initialization** - Environment setup and health checks
2. **Phase 1: Schema Discovery** - Automated schema mapping and field identification
3. **Phase 2: Data Aggregation** - User-daily aggregation with comprehensive metrics
4. **Phase 3: User Segmentation** - Statistical user segmentation (placeholder)
5. **Phase 4: Quality Assurance** - Data validation and sanity checks (placeholder)
6. **Phase 5: LLM Insights** - AI-powered insight generation (placeholder)
7. **Phase 6: Final Reporting** - Comprehensive report generation

## 📊 Core Features

- **Automated Schema Discovery**: AI-powered field mapping and data quality assessment
- **Comprehensive Data Aggregation**: User-daily metrics with session duration and revenue classification
- **Statistical Rigor**: Built-in statistical validation and significance testing
- **Memory System**: Knowledge accumulation and learning from past analyses
- **Cost Optimization**: Intelligent query optimization and resource management
- **Quality Assurance**: Basic data validation and sanity checks
- **Reproducible Results**: Version-controlled, traceable analysis runs

## 🔧 Dependencies

### Required Packages
```bash
pip install google-cloud-bigquery google-oauth2 pandas python-dotenv scipy openai==1.30.0 db-dtypes matplotlib seaborn
```

**Package Details:**
- **google-cloud-bigquery**: BigQuery client for data extraction
- **google-oauth2**: Authentication for Google Cloud services
- **pandas**: Data manipulation and analysis
- **python-dotenv**: Environment variable management
- **scipy**: Statistical analysis (used in user segmentation)
- **openai==1.30.0**: OpenAI API client for LLM insights generation (pinned to 1.30.0 for compatibility)
- **db-dtypes**: Required for BigQuery `to_dataframe()` method to handle BigQuery data types
- **matplotlib**: Chart and visualization generation
- **seaborn**: Statistical data visualization (used for enhanced chart styling)

### Environment Variables
- `DATASET_NAME`: BigQuery dataset name
- `GOOGLE_CLOUD_PROJECT`: Google Cloud project ID
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account credentials
- `RUN_HASH`: Unique identifier for analysis runs
 - `OPENAI_API_KEY`: OpenAI key (optional; also read from creds.json)

## 📁 Project Structure

```
project-root/
├── workflow_guidelines/           # 📚 Core documentation and guidelines
│   ├── script_summary/           # 📖 Comprehensive script documentation
│   ├── REPOSITORY_STRUCTURE_AND_RULES.md
│   ├── high-level-design.md
│   ├── sequenceDiagram.md
│   └── data_aggregation_table_structure.md
├── scripts/                      # 🔧 Implementation scripts
├── run_logs/                     # 📊 Analysis run outputs
├── tests/                        # 🧪 Test files
├── analysis-workflow.md          # 📋 Workflow execution guide
├── backlog.md                    # 📝 Future improvements
└── README.md                     # 📖 This file
```

## 🚀 Getting Started

1. **Clone the repository**
2. **Set up environment variables** (see `.env.template`)
3. **Install dependencies** (`pip install -r requirements.txt`)
4. **Read the script documentation** in `workflow_guidelines/script_summary/`
5. **Run your first analysis** with `python scripts/analysis_workflow_orchestrator.py`

## 📖 Additional Resources

- **[Analysis Workflow](./analysis-workflow.md)** - Detailed workflow execution guide
- **[Backlog](./backlog.md)** - Future improvements and enhancements
- **[Script Inventory](./scripts/SCRIPT_INVENTORY.md)** - Complete inventory of all scripts
- **[Script Versioning](./scripts/SCRIPT_VERSIONING.md)** - Versioning guidelines and history

## 🤝 Contributing

Please refer to the [Repository Structure and Rules](./workflow_guidelines/REPOSITORY_STRUCTURE_AND_RULES.md) for development standards and contribution guidelines.

## 📄 License

This project is part of the Product Dashboard Builder v2 system for automated data analysis and due diligence.
