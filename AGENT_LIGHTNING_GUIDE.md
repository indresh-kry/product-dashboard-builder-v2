# Agent Lightning Integration Guide

## Overview

This project has been integrated with Agent Lightning for automatic prompt optimization (Phase 3). The integration is **optional** - the system works with a fallback implementation when Agent Lightning is not installed, but using Agent Lightning provides more advanced optimization capabilities.

## Current Status

✅ **Phase 3 Framework**: Fully implemented and working  
⚠️ **Agent Lightning Library**: Not installed (optional enhancement)  
✅ **Fallback APO**: Working without Agent Lightning

The system automatically detects if Agent Lightning is available and uses it when present, otherwise falls back to a built-in optimization implementation.

---

## Installation

### Option 1: Install Agent Lightning from Microsoft Research

```bash
# Install from PyPI (if available)
pip install agent-lightning

# OR install from source
git clone https://github.com/microsoft/agent-lightning.git
cd agent-lightning
pip install -e .
```

### Option 2: Check if Already Installed

```bash
python3 -c "from agent_lightning import Agent, APOAlgorithm; print('✅ Agent Lightning installed')"
```

If you see `⚠️ Agent Lightning not available. Using fallback APO implementation.`, it means Agent Lightning is not installed.

---

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Phase 3 Integration Layer                  │
│  (prompt_optimization/phase3_integration.py)            │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   APO Optimizer                │
         │  (apo_optimizer.py)            │
         └───────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         │                               │
         ▼                               ▼
┌──────────────────┐          ┌──────────────────┐
│ Agent Lightning   │          │ Fallback APO     │
│ (if installed)   │          │ (always available)│
│                   │          │                  │
│ - Advanced        │          │ - Prompt        │
│   optimization    │          │   variations    │
│ - RL algorithms   │          │ - Reward-based  │
│ - Meta-learning   │          │   selection     │
└──────────────────┘          └──────────────────┘
```

### Integration Points

1. **Automatic Detection**: The system automatically detects if Agent Lightning is available
2. **Graceful Fallback**: If not available, uses built-in optimization
3. **Same Interface**: Both implementations use the same API, so switching is seamless

---

## Usage

### 1. Enable Phase 3 Optimization

Phase 3 is controlled by the `ENABLE_PHASE3_OPTIMIZATION` environment variable:

```bash
export ENABLE_PHASE3_OPTIMIZATION=1
```

### 2. Automatic Performance Tracking

When Phase 3 is enabled, **every agent run automatically tracks performance**:

```bash
export ENABLE_PHASE3_OPTIMIZATION=1
export STRICT_VALIDATION=true

# Run agents normally - performance is tracked automatically
python3 -m agents.agentic_coordinator \
  --run-hash 364152 \
  --app-filter "ALL_APPS" \
  --date-start "2025-09-21" \
  --date-end "2025-10-05"
```

**What happens:**
- Each agent's LLM response is scored using the reward function
- Performance metrics are saved to `prompt_versions/performance.json`
- Prompt versions are tracked in `prompt_versions/versions.json`

### 3. Run Prompt Optimization

#### With Agent Lightning (if installed):

```bash
export ENABLE_PHASE3_OPTIMIZATION=1

# Optimize prompts using Agent Lightning APO
python3 -m agents.optimize_prompts \
  --agent-type daily_metrics \
  --steps 100 \
  --run-logs-dir run_logs
```

The optimizer will:
1. Load historical training data from `run_logs/`
2. Use Agent Lightning's APO algorithm (if available)
3. Generate optimized prompt variations
4. Test them on historical data
5. Save the best version to `prompt_versions/`

#### Without Agent Lightning (fallback):

The same command works but uses the fallback implementation:
- Still loads training data
- Uses simpler variation-based optimization
- Still generates and tests prompt variations
- Still saves optimized versions

### 4. Check Optimization Results

```python
from agents.prompt_optimization import PromptVersionManager

manager = PromptVersionManager()

# Get performance summary for an agent
summary = manager.get_performance_summary('daily_metrics')
print(f"Total versions: {summary['total_versions']}")
print(f"Best version: {summary['best_version']}")
print(f"Best avg reward: {summary['best_avg_reward']:.3f}")

# Get all versions
all_versions = manager.get_all_versions('daily_metrics')
for version_id, version_data in all_versions.items():
    print(f"{version_id}: {version_data['created_at']}")
```

### 5. View Stored Versions

```bash
# View prompt versions
cat prompt_versions/versions.json

# View performance metrics
cat prompt_versions/performance.json
```

---

## Agent Lightning API Usage

If Agent Lightning is installed, you can use it directly:

### Example: Using Agent Lightning APO

```python
from agent_lightning import Agent, APOAlgorithm
from agents.prompt_optimization import PromptRewardFunction
from agents.prompt_optimization import TrainingDataLoader

# Load training data
loader = TrainingDataLoader()
training_data = loader.get_training_data_for_agent('daily_metrics', limit=20)

# Create reward function
reward_fn = PromptRewardFunction()

# Define agent (wraps your LLM agent)
class DailyMetricsAgent(Agent):
    def __call__(self, prompt: str, data: Dict) -> Dict:
        # Your existing LLM call logic
        return self.llm_client.call(system_prompt, prompt)
    
    def reward_function(self, response: Dict) -> float:
        return reward_fn.calculate_reward(response)

# Initialize APO
agent = DailyMetricsAgent()
apo = APOAlgorithm(
    agent=agent,
    reward_function=lambda r: agent.reward_function(r),
    optimization_steps=100
)

# Optimize
initial_prompts = {
    'system_prompt': '...',
    'user_prompt_template': '...'
}

optimized = apo.optimize(
    initial_prompts=initial_prompts,
    training_data=training_data
)
```

---

## Configuration

### Environment Variables

```bash
# Enable Phase 3 optimization
export ENABLE_PHASE3_OPTIMIZATION=1

# Enable strict validation (recommended)
export STRICT_VALIDATION=true

# Optional: Use Agent Lightning explicitly
export USE_AGENT_LIGHTNING=true  # Falls back if not available
```

### Optimization Settings

The optimizer can be configured in `apo_optimizer.py`:

```python
# Default settings
optimization_steps = 100          # Number of optimization iterations
temperature_variations = 0.1      # Temperature for prompt variations
min_specificity_score = 6.5      # Minimum specificity (strict mode)
min_actionability_score = 6.0    # Minimum actionability (strict mode)
```

---

## What Agent Lightning Provides

### Advantages of Agent Lightning

1. **Advanced Optimization Algorithms**
   - More sophisticated prompt variation strategies
   - Meta-learning capabilities
   - Better exploration of prompt space

2. **Reinforcement Learning**
   - Can learn from historical performance
   - Adapts optimization strategy over time
   - Better convergence to optimal prompts

3. **Research-Backed Techniques**
   - Based on Microsoft Research
   - Proven optimization methods
   - Continuous improvements

### Fallback Implementation

The fallback provides:
- ✅ Basic prompt variation generation
- ✅ Reward-based selection
- ✅ Version tracking
- ✅ Performance monitoring
- ⚠️ Simpler optimization (no RL, no meta-learning)

---

## Step-by-Step: First Time Setup

### 1. Install Agent Lightning (Optional)

```bash
pip install agent-lightning
# OR from source (see Installation section)
```

### 2. Verify Installation

```bash
python3 -c "from agent_lightning import Agent, APOAlgorithm; print('✅ Agent Lightning ready')"
```

### 3. Generate Training Data

Run agents on multiple runs to build training dataset:

```bash
# Run 1
python3 analysis_workflow_orchestrator_unified.py \
  --app-filter "ALL_APPS" \
  --date-start "2025-08-01" \
  --date-end "2025-08-15" \
  --aggregation-limit 750000

# Run 2
python3 analysis_workflow_orchestrator_unified.py \
  --app-filter "ALL_APPS" \
  --date-start "2025-08-15" \
  --date-end "2025-08-31" \
  --aggregation-limit 750000

# Run 3 (your current run)
python3 analysis_workflow_orchestrator_unified.py \
  --app-filter "ALL_APPS" \
  --date-start "2025-09-21" \
  --date-end "2025-10-05" \
  --aggregation-limit 750000
```

### 4. Run Optimization

```bash
export ENABLE_PHASE3_OPTIMIZATION=1

# Optimize each agent
python3 -m agents.optimize_prompts --agent-type daily_metrics --steps 100
python3 -m agents.optimize_prompts --agent-type user_segmentation --steps 100
python3 -m agents.optimize_prompts --agent-type revenue_optimization --steps 100
# ... etc for all agents
```

### 5. Use Optimized Prompts

Optimized prompts are automatically used in future runs when Phase 3 is enabled:

```bash
export ENABLE_PHASE3_OPTIMIZATION=1

# Next run will use optimized prompts automatically
python3 -m agents.agentic_coordinator --run-hash <new_hash> ...
```

---

## Monitoring and Evaluation

### Check Optimization Progress

```bash
# View all prompt versions
python3 -c "
from agents.prompt_optimization import PromptVersionManager
m = PromptVersionManager()
for agent in ['daily_metrics', 'user_segmentation', 'revenue_optimization']:
    summary = m.get_performance_summary(agent)
    print(f'{agent}: {summary.get(\"best_avg_reward\", 0):.3f} avg reward')
"
```

### Compare Versions

```python
from agents.prompt_optimization import PromptVersionManager

manager = PromptVersionManager()

# Get best version
best = manager.get_best_version('daily_metrics')
best_data = manager.get_prompt_version('daily_metrics', best)

print(f"Best version: {best}")
print(f"System prompt preview: {best_data['system_prompt'][:200]}...")
```

### Performance Metrics

All metrics are stored in `prompt_versions/performance.json`:

```json
{
  "daily_metrics:initial": {
    "agent_type": "daily_metrics",
    "version_id": "initial",
    "avg_reward": 0.65,
    "total_runs": 5,
    "runs": [...]
  },
  "daily_metrics:step_50": {
    "agent_type": "daily_metrics",
    "version_id": "step_50",
    "avg_reward": 0.72,
    "total_runs": 3,
    "runs": [...]
  }
}
```

---

## Troubleshooting

### Agent Lightning Not Detected

**Symptom:** `⚠️ Agent Lightning not available. Using fallback APO implementation.`

**Solutions:**
1. Install Agent Lightning: `pip install agent-lightning`
2. Check Python path: `python3 -c "import sys; print(sys.path)"`
3. Verify installation: `python3 -c "from agent_lightning import Agent"`

### No Training Data

**Symptom:** `⚠️ No training data found for {agent_type}`

**Solution:**
- Run agents on multiple runs first to generate training data
- Training data comes from `run_logs/{hash}/outputs/insights/agentic_insights.json`

### Optimization Not Running

**Symptom:** Prompts not being optimized

**Check:**
```bash
# Verify Phase 3 is enabled
echo $ENABLE_PHASE3_OPTIMIZATION  # Should be "1"

# Check if optimized versions exist
ls -la prompt_versions/versions.json
```

---

## Best Practices

1. **Generate Sufficient Training Data**
   - Run agents on at least 5-10 different runs
   - More training data = better optimization

2. **Run Optimization Periodically**
   - After every 5-10 new runs
   - When prompt performance degrades
   - When adding new agent types

3. **Monitor Performance**
   - Check average rewards regularly
   - Compare optimized vs original versions
   - A/B test in production if possible

4. **Use Strict Validation**
   - Always enable `STRICT_VALIDATION=true`
   - Ensures high-quality recommendations
   - Better training signals for optimization

5. **Version Control**
   - `prompt_versions/` contains all versions
   - Can rollback if optimization degrades performance
   - Keep versions for analysis

---

## Advanced Usage

### Custom Reward Functions

You can customize the reward function:

```python
from agents.prompt_optimization import PromptRewardFunction

class CustomRewardFunction(PromptRewardFunction):
    def calculate_reward(self, llm_response, weights=None):
        # Custom scoring logic
        base_reward = super().calculate_reward(llm_response, weights)
        # Add custom adjustments
        return base_reward * 1.1  # Example: 10% bonus
```

### Direct Agent Lightning Integration

If you want to use Agent Lightning directly without the wrapper:

```python
from agent_lightning import Agent, APOAlgorithm, RLAlgorithm
from agents.prompt_optimization.reward_function import PromptRewardFunction

# Create custom agent
class MyAgent(Agent):
    # Implement __call__ and reward_function
    pass

# Use APO
apo = APOAlgorithm(agent=MyAgent(), ...)

# OR use RL
rl = RLAlgorithm(agent=MyAgent(), ...)
```

---

## Summary

**Current State:**
- ✅ Phase 3 framework fully implemented
- ✅ Automatic performance tracking working
- ✅ Fallback APO implementation functional
- ⚠️ Agent Lightning library optional (not required)

**To Use Agent Lightning:**
1. Install: `pip install agent-lightning`
2. Enable: `export ENABLE_PHASE3_OPTIMIZATION=1`
3. Run optimization: `python3 -m agents.optimize_prompts --agent-type daily_metrics --steps 100`

**Benefits:**
- Automatic prompt optimization
- Better recommendations over time
- Data-driven prompt improvement
- Works with or without Agent Lightning

The system is designed to work seamlessly whether Agent Lightning is installed or not!

