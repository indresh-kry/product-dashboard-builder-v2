# Agent Lightning Quick Start Guide

## Quick Answer: How to Use Agent Lightning

### Current Status
- ✅ **Framework Ready**: Phase 3 integration is complete
- ⚠️ **Agent Lightning**: Not installed (optional)
- ✅ **Fallback Working**: System works without Agent Lightning

### Three Ways to Use It

#### 1. **Use Without Agent Lightning (Current Setup)**
The system works perfectly without Agent Lightning using a built-in optimization:

```bash
export ENABLE_PHASE3_OPTIMIZATION=1
python3 -m agents.optimize_prompts --agent-type daily_metrics --steps 50
```

**What you get:**
- ✅ Automatic performance tracking
- ✅ Prompt versioning
- ✅ Basic prompt optimization
- ✅ Reward-based selection

#### 2. **Install Agent Lightning for Enhanced Optimization**

```bash
# Install Agent Lightning
pip install agent-lightning

# Verify installation
python3 -c "from agent_lightning import Agent, APOAlgorithm; print('✅ Ready')"

# Use with Agent Lightning (automatic detection)
export ENABLE_PHASE3_OPTIMIZATION=1
python3 -m agents.optimize_prompts --agent-type daily_metrics --steps 100
```

**What you get additionally:**
- ✅ Advanced optimization algorithms
- ✅ Reinforcement learning capabilities
- ✅ Better prompt space exploration
- ✅ Research-backed techniques

#### 3. **Enable Automatic Optimization During Runs**

Performance tracking happens automatically:

```bash
export ENABLE_PHASE3_OPTIMIZATION=1
export STRICT_VALIDATION=true

# Every agent run tracks performance automatically
python3 -m agents.agentic_coordinator --run-hash <hash> ...
```

**What happens:**
- Each agent's output is scored (0.0-1.0)
- Performance saved to `prompt_versions/performance.json`
- Can later optimize based on this data

---

## Key Integration Points

### 1. Automatic Detection
The system automatically detects if Agent Lightning is installed:

```python
# In apo_optimizer.py
try:
    from agent_lightning import Agent, APOAlgorithm
    AGENT_LIGHTNING_AVAILABLE = True
except ImportError:
    AGENT_LIGHTNING_AVAILABLE = False
    # Falls back to built-in optimization
```

### 2. Reward Function
Uses your existing `RecommendationValidator`:

```python
# Calculates reward based on:
# - Specificity (40%)
# - Actionability (40%)
# - Data Support (20%)
reward = (specificity * 0.4) + (actionability * 0.4) + (data_support * 0.2)
```

### 3. Version Management
All versions tracked in:
- `prompt_versions/versions.json` - Prompt versions
- `prompt_versions/performance.json` - Performance metrics

### 4. Training Data
Automatically loads from:
- `run_logs/{hash}/outputs/insights/agentic_insights.json`

---

## Example Workflow

### Step 1: Generate Training Data
```bash
# Run multiple analyses to build training dataset
python3 analysis_workflow_orchestrator_unified.py \
  --app-filter "ALL_APPS" \
  --date-start "2025-09-01" --date-end "2025-09-15" \
  --aggregation-limit 750000

python3 analysis_workflow_orchestrator_unified.py \
  --app-filter "ALL_APPS" \
  --date-start "2025-09-15" --date-end "2025-09-30" \
  --aggregation-limit 750000
```

### Step 2: Run Optimization
```bash
export ENABLE_PHASE3_OPTIMIZATION=1

# Optimize prompts (works with or without Agent Lightning)
python3 -m agents.optimize_prompts \
  --agent-type daily_metrics \
  --steps 100 \
  --run-logs-dir run_logs
```

### Step 3: Use Optimized Prompts
```bash
# Optimized prompts are automatically used in future runs
export ENABLE_PHASE3_OPTIMIZATION=1

python3 -m agents.agentic_coordinator \
  --run-hash <new_hash> \
  --app-filter "ALL_APPS" \
  --date-start "2025-10-01" \
  --date-end "2025-10-15"
```

---

## Key Files

- **`AGENT_LIGHTNING_GUIDE.md`** - Complete guide (this file)
- **`scripts/agents/prompt_optimization/apo_optimizer.py`** - Main optimization logic
- **`scripts/agents/prompt_optimization/phase3_integration.py`** - Integration layer
- **`scripts/agents/optimize_prompts.py`** - Optimization script

---

## What You Get

### Without Agent Lightning:
- ✅ Works immediately
- ✅ Basic optimization
- ✅ Performance tracking
- ✅ Version management

### With Agent Lightning:
- ✅ Everything above, PLUS
- ✅ Advanced algorithms
- ✅ Better optimization
- ✅ Research-backed methods

**Bottom Line**: The system works great either way. Agent Lightning is an optional enhancement that provides better optimization when installed.

