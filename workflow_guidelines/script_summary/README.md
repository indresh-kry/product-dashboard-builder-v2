# Script Summary Documentation

This directory contains detailed documentation for all active scripts in the Product Dashboard Builder v2 system.

## Purpose

Each script documentation includes:
- **Function Overview**: All functions defined in the script with parameters and descriptions
- **Tools & External Dependencies**: External tools, libraries, and APIs with detailed descriptions
- **Variables & Configuration**: Complete variable documentation by source (input, env, hardcoded, computed)
- **Flow Diagram**: Mermaid diagram showing function call flow
- **Usage Examples**: How to use the script with command-line options
- **Dependencies**: Required packages, libraries, and environment variables

## Scripts Documented

### Core Workflow Scripts
- **[analysis_workflow_orchestrator.md](./analysis_workflow_orchestrator.md)** - Central orchestrator for entire workflow
- **[schema_discovery_v3.md](./schema_discovery_v3.md)** - Enhanced schema discovery with session/revenue analysis
- **[data_aggregation_v3.md](./data_aggregation_v3.md)** - Final working data aggregation with all fixes

### Supporting Scripts
- **[system_health_check.md](./system_health_check.md)** - System health validation and environment checks
- **[rules_engine_integration.md](./rules_engine_integration.md)** - Rules engine integration for business logic

## Documentation Structure

Each script documentation follows this structure:

1. **Script Overview** - Purpose, version, and main functionality
2. **Functions** - All functions with descriptions, parameters, and return values
3. **Tools & External Dependencies** - External tools, libraries, and APIs with detailed descriptions
4. **Variables & Configuration** - Complete variable documentation organized by source:
   - Input Variables (Function Parameters)
   - Environment Variables (from .env files)
   - Hardcoded Variables
   - Computed Variables
5. **Flow Diagram** - Mermaid diagram of function call flow
6. **Usage Examples** - How to execute the script with command-line options
7. **Dependencies** - Required packages, libraries, and environment variables

## Template

For new scripts, use the **[SCRIPT_DOCUMENTATION_TEMPLATE.md](./SCRIPT_DOCUMENTATION_TEMPLATE.md)** to ensure consistent documentation structure.

## Maintenance

These documents should be updated when:
- New functions are added to scripts
- External dependencies change
- Script flow or logic is modified
- New integrations are added

---

*This documentation ensures developers understand the internal workings of each script and can effectively maintain and extend the system.*
