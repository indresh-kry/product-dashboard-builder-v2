# Prompt Optimization Approach

## Executive Summary

This document outlines a comprehensive approach to improve LLM prompt quality and output usefulness for the product dashboard insights report. The analysis is based on:
1. Current codebase structure and prompt implementation
2. Agent Lightning capabilities (Microsoft Research)
3. General prompt engineering best practices
4. Goal: Eliminate generic recommendations and make outputs actionable

---

## Current State Analysis

### Architecture Overview
- **6 Specialized Child Agents**: Daily Metrics, User Segmentation, Geographic, Cohort Retention, Revenue Optimization, Data Quality
- **Agentic Coordinator**: Aggregates results, generates markdown report (NO LLM synthesis)
- **Prompt Structure**: System prompts + data formatting + analysis instructions
- **Output Format**: Structured JSON with insights, recommendations, evidence

### Current Issues Identified

#### 1. Generic Recommendations Problem
Examples from current output:
- ❌ "Implement re-engagement campaigns and analyze churn reasons"
- ❌ "Develop targeted upsell strategies and premium features"
- ❌ "Introduce gamification elements to increase engagement"
- ❌ "Deploy machine learning models to predict churn risk"

**Why they're generic:**
- No specific implementation details
- No reference to actual data patterns
- Could apply to any product/industry
- No measurable, time-bound actions

#### 2. Structural Issues

**Prompt Structure:**
- System prompts are too broad ("analyze revenue patterns")
- Instructions lack specificity on what "actionable" means
- No examples of good vs bad recommendations
- Missing domain-specific constraints (what NOT to recommend)

**Data Formatting:**
- Raw data dump (DataFrame.to_string()) may overwhelm LLM
- No pre-processing or highlighting of key patterns
- No statistical summaries or anomaly flags

**No Synthesis Layer:**
- Each agent operates independently
- No coordinator LLM to prioritize/cross-reference insights
- Recommendations may conflict or duplicate
- No executive-level prioritization

**Output Structure:**
- JSON format is rigid but not enforced with validation
- Evidence provided but not always quantified
- No confidence scoring or uncertainty quantification

---

## Proposed Improvements

### Phase 1: Prompt Structure & Quality Improvements

#### 1.1 Enhanced System Prompts

**Current:** Generic role definition
```
"You are a data analyst specializing in daily metrics analysis..."
```

**Proposed:** Role + constraints + examples
```
"You are a product analytics consultant with 10+ years experience in mobile games.

CRITICAL CONSTRAINTS:
- NEVER recommend: 'add features', 'improve UX', 'use ML/AI' (too generic)
- ALWAYS specify: WHAT metric, WHEN to measure, EXPECTED change, TIME frame
- REQUIRED format: 'For [segment/date], [action] targeting [specific users] 
  by [date] expecting [metric] change from [current] to [target] within [timeframe]'

GOOD RECOMMENDATION EXAMPLE:
'For high-engagement users (46.6% of base), implement a premium tier unlock 
prompt on day 3 post-install (when 67% reach level 5). Target ARPU increase 
from $0.15 to $0.35 within 30 days based on similar user conversion rates.'

BAD RECOMMENDATION (DO NOT GENERATE):
'Develop targeted upsell strategies for high engagement users' (too vague)

OUTPUT REQUIREMENTS:
1. Each recommendation MUST reference:
   - Exact metric name and current value from data
   - Target value with confidence interval
   - Specific date/cohort/segment from data
   - Expected timeline (days/weeks, not 'quarter')
2. If data doesn't support a specific recommendation, state 'INSUFFICIENT DATA' 
   rather than generic advice
"
```

#### 1.2 Few-Shot Examples in Prompts

**Add examples to each prompt generator:**

```python
def get_few_shot_examples(self) -> str:
    return """
EXAMPLES OF GOOD VS BAD INSIGHTS:

BAD:
{
  "recommendation": "Improve user retention",
  "evidence": "Low retention rates"
}

GOOD:
{
  "recommendation": "Target cohort 2025-08-15 (43.57% D1 retention vs 30% avg): 
                     Replicate onboarding flow used that day - increased D7 retention 
                     by 12% vs baseline",
  "evidence": "Cohort 2025-08-15 shows 43.57% D1 (vs 30% avg), $1439.60 day-0 revenue 
               (highest). Compare funnel events day-0 vs day-1 for this cohort only.",
  "specificity_score": 9.5,
  "actionability_score": 9.0,
  "data_support_score": 8.5
}

BAD:
{
  "recommendation": "Implement machine learning models",
  "evidence": "Churn rate is high"
}

GOOD:
{
  "recommendation": "Churned users (18.5% of base) show 0 revenue events after day 3. 
                     Set up email push campaign triggered on day 3 if no IAP, offering 
                     20% discount on first purchase. Test with 5,000 users next week.",
  "evidence": "18.5% churn rate. Cross-reference: churned_users.csv shows 89% had 
               last_active_date within 3 days of install. Expected conversion: 8-12% 
               (industry benchmark for day-3 discount campaigns).",
  "specificity_score": 9.8,
  "actionability_score": 9.5,
  "data_support_score": 9.0
}
"""
```

#### 1.3 Structured Prompt Template with Validation

**Create a prompt template with required fields:**

```python
class ActionableRecommendationTemplate:
    """Template to enforce specific recommendation structure"""
    
    REQUIRED_FIELDS = {
        'what': 'Specific action (verb + object)',
        'who': 'Target segment/user group (with % and size)',
        'when': 'Timing/trigger (specific date or condition)',
        'where': 'Geographic/channel if applicable',
        'expected_outcome': 'Metric name, current value → target value',
        'timeframe': 'Days/weeks, not vague terms',
        'confidence': 'High/Medium/Low with rationale',
        'data_requirement': 'What additional data needed if any',
        'implementation_complexity': 'Simple/Medium/Complex'
    }
    
    GENERIC_PHRASES_TO_AVOID = [
        'improve', 'enhance', 'optimize', 'implement strategies',
        'deploy models', 'use AI/ML', 'build features', 'add functionality'
    ]
```

#### 1.4 Data Pre-processing for Context

**Instead of raw DataFrame.to_string(), add:**

```python
def format_data_with_insights(self, data: pd.DataFrame) -> str:
    """Format data with statistical highlights"""
    
    # Calculate key stats
    stats = {
        'trend': self._detect_trend(data),
        'anomalies': self._detect_anomalies(data),
        'top_performers': self._get_top_n(data, n=5),
        'bottom_performers': self._get_bottom_n(data, n=5),
        'correlations': self._find_correlations(data)
    }
    
    # Format with context
    return f"""
**Data Summary:**
- Total records: {len(data)}
- Date range: {data['date'].min()} to {data['date'].max()}
- Trend: {stats['trend']['direction']} ({stats['trend']['change_pct']}% change)

**Key Anomalies Detected:**
{self._format_anomalies(stats['anomalies'])}

**Top Performers (Focus Areas):**
{self._format_performers(stats['top_performers'])}

**Full Dataset:**
{data.to_string(index=False)}
"""
```

---

### Phase 2: Architectural Improvements

#### 2.1 Add Coordinator LLM (Parent Agent)

**Current:** Coordinator just aggregates and formats

**Proposed:** Add synthesis LLM

```python
class CoordinatorLLMAgent:
    """Parent LLM that synthesizes child agent insights"""
    
    def synthesize_insights(self, child_results: Dict) -> Dict:
        """
        Takes all child agent outputs and:
        1. Identifies overlapping/redundant recommendations
        2. Prioritizes by impact and feasibility
        3. Creates executive summary
        4. Flags conflicts (e.g., one agent says increase IAP, another says focus on ads)
        """
        
        system_prompt = """
You are an executive product strategy consultant synthesizing insights from 
6 specialized analysts:

1. Daily Metrics Analyst - trends, anomalies, daily patterns
2. User Segmentation Analyst - user behavior, segments, churn
3. Geographic Analyst - regional performance
4. Cohort Retention Analyst - lifecycle, retention patterns
5. Revenue Optimization Analyst - revenue streams, monetization
6. Data Quality Analyst - data integrity, completeness

YOUR TASK:
1. Identify the TOP 5 most impactful recommendations across all analysts
2. Resolve conflicts (e.g., if one says "focus on IAP" and another says "focus on ads")
3. Rank by: Expected Impact × Confidence × Implementation Feasibility
4. Create executive summary highlighting:
   - Most critical issue requiring immediate action
   - Highest ROI opportunity
   - Data quality concerns affecting decisions
   - Missing data needed for better insights

OUTPUT FORMAT:
{
  "executive_summary": "...",
  "top_5_prioritized_recommendations": [...],
  "conflicts_resolved": [...],
  "data_gaps": [...],
  "risk_assessments": [...]
}
"""
```

#### 2.2 Chain-of-Thought Prompting

**Add reasoning steps to prompts:**

```python
def get_cot_instructions(self) -> str:
    return """
ANALYSIS PROCESS (think step by step):

STEP 1: Data Inspection
- What are the actual numbers? List top 3 metrics and their values
- What are the trends? Increasing/decreasing/stable?
- Are there anomalies? List specific dates/values that stand out

STEP 2: Pattern Identification
- What patterns do you see? (Be specific: "Revenue drops 40% on weekends" not "revenue varies")
- Which segments/cohorts/regions perform differently? (List with numbers)

STEP 3: Root Cause Hypothesis
- Why might these patterns exist? (Based on data, not assumptions)
- What data supports your hypothesis? (Cite specific rows/values)

STEP 4: Recommendation Generation
- What specific action addresses the root cause?
- Who should take action? (Specific team/role, not "product team")
- When? (Specific date or trigger, not "soon")
- Expected outcome? (Metric: current → target with confidence interval)

STEP 5: Validation
- Can this recommendation be executed next week? (If no, refine)
- Is it specific to THIS dataset? (If generic, discard)
- Does it reference actual data points? (If no, discard)
"""
```

#### 2.3 Output Validation & Filtering

**Add validation layer before saving:**

```python
class RecommendationValidator:
    """Validates recommendations meet quality criteria"""
    
    def validate(self, recommendation: Dict) -> Dict:
        """
        Returns:
        {
            'is_valid': bool,
            'specificity_score': float (0-10),
            'actionability_score': float (0-10),
            'data_support_score': float (0-10),
            'issues': [list of problems],
            'suggestions': [how to improve]
        }
        """
        
        checks = {
            'has_specific_action': self._check_action_specificity(recommendation),
            'has_target_metric': self._check_metric_target(recommendation),
            'has_timeframe': self._check_timeframe(recommendation),
            'has_data_evidence': self._check_evidence(recommendation),
            'not_generic': self._check_generic_phrases(recommendation),
            'references_data': self._check_data_references(recommendation)
        }
        
        return self._calculate_scores(checks)
```

---

### Phase 3: Integration with Agent Lightning

#### 3.1 Automatic Prompt Optimization (APO)

**Agent Lightning supports APO with minimal code changes:**

```python
# Wrap existing agents with Agent Lightning
from agent_lightning import Agent, APOAlgorithm

# Convert existing LLMAgent to Agent Lightning format
class DailyMetricsAgent(Agent):
    def __call__(self, prompt: str, data: Dict) -> Dict:
        # Your existing analyze_with_llm logic
        return self.llm_client.call(system_prompt, prompt)
    
    def reward_function(self, response: Dict) -> float:
        """
        Define reward based on output quality:
        - High reward: Specific, actionable, data-driven
        - Low reward: Generic, vague, no evidence
        """
        validator = RecommendationValidator()
        scores = validator.validate(response)
        
        # Weighted reward
        reward = (
            scores['specificity_score'] * 0.4 +
            scores['actionability_score'] * 0.4 +
            scores['data_support_score'] * 0.2
        )
        
        return reward / 10.0  # Normalize to 0-1

# Initialize APO
apo = APOAlgorithm(
    agent=DailyMetricsAgent(),
    reward_function=lambda r: agent.reward_function(r),
    optimization_steps=100
)

# Train prompt optimization
optimized_prompts = apo.optimize(
    initial_prompts=current_prompts,
    training_data=historical_runs
)
```

#### 3.2 Reinforcement Learning for Prompt Improvement

**Use RL to learn better prompts from feedback:**

```python
from agent_lightning import RLAlgorithm

# Define action space: prompt variations
# Define state: current data + prompt
# Define reward: user feedback or validation scores

rl_trainer = RLAlgorithm(
    agent=DailyMetricsAgent(),
    state_space=['data_summary', 'prompt_template', 'previous_outputs'],
    action_space=['prompt_modifications'],
    reward_function=validation_reward_function
)

# Learn from historical runs
rl_trainer.train(
    episodes=1000,
    historical_data=load_previous_runs()
)
```

**Benefits:**
- Automatic discovery of better prompt formulations
- Learning from what worked well in past runs
- Continuous improvement without manual tuning

#### 3.3 Implementation Strategy

**Phase 3A: Proof of Concept (1 agent)**
- Start with `daily_metrics` agent
- Wrap with Agent Lightning APO
- Run optimization on historical data
- Compare optimized vs original prompts

**Phase 3B: Scale to All Agents**
- Apply to all 6 agents
- Maintain agent-specific reward functions
- Create unified prompt library

**Phase 3C: Add Coordinator LLM Optimization**
- Optimize coordinator synthesis prompt
- Learn best prioritization strategies

---

## Implementation Roadmap

### Phase 1: Quick Wins (Week 1-2)
**Goal:** Improve prompt quality without infrastructure changes

1. **Update all system prompts** with:
   - Specific constraints (what NOT to recommend)
   - Few-shot examples (good vs bad)
   - Chain-of-thought instructions

2. **Add recommendation validator**:
   - Score outputs for specificity/actionability
   - Filter out generic recommendations before saving

3. **Enhance data formatting**:
   - Add statistical summaries
   - Highlight anomalies
   - Pre-process to show key patterns

**Expected Impact:** 30-40% reduction in generic recommendations

---

### Phase 2: Architecture Improvements (Week 3-4)
**Goal:** Add synthesis and validation layers

1. **Implement Coordinator LLM**:
   - Synthesize insights from all 6 agents
   - Resolve conflicts
   - Prioritize recommendations

2. **Add output validation**:
   - Real-time scoring
   - Automatic filtering
   - Quality metrics in output

3. **Improve report generation**:
   - Executive summary from coordinator
   - Prioritized action items
   - Confidence levels

**Expected Impact:** 60-70% improvement in output usefulness

---

### Phase 3: Agent Lightning Integration (Week 5-8)
**Goal:** Automatic prompt optimization

1. **Setup Agent Lightning**:
   - Install and configure
   - Wrap existing agents
   - Define reward functions

2. **Run APO**:
   - Start with 1 agent (proof of concept)
   - Collect optimization results
   - Compare before/after

3. **Scale**:
   - Apply to all agents
   - Continuous optimization
   - A/B testing framework

**Expected Impact:** 80-90% improvement in output quality

---

## Key Design Decisions

### 1. Should we replace prompts or optimize existing ones?
**Decision:** Optimize existing prompts first (Phase 1-2), then use Agent Lightning for continuous improvement (Phase 3).

**Rationale:** 
- Quick wins validate approach
- Agent Lightning learns from what works
- Lower risk than full replacement

### 2. How to handle conflicting recommendations?
**Decision:** Add Coordinator LLM (Phase 2) to synthesize and resolve conflicts.

**Rationale:**
- Multiple agents may suggest contradictory actions
- Need executive-level prioritization
- Better than manual review

### 3. What's the reward function for Agent Lightning?
**Decision:** Multi-factor scoring:
- Specificity (40%)
- Actionability (40%) 
- Data support (20%)

**Rationale:**
- Balanced approach
- Measurable criteria
- Can adjust weights based on results

### 4. How to measure success?
**Decision:** Track:
- Generic recommendation rate (target: <10%)
- Specificity scores (target: >8/10)
- Actionability scores (target: >8/10)
- User feedback (if available)

**Rationale:**
- Objective metrics
- Can A/B test improvements
- Continuous monitoring

---

## Open Questions & Clarifications Needed

### Technical
1. **Agent Lightning Integration:**
   - Should we use APO, RL, or both?
   - What's the training data source? (Historical runs? Synthetic?)
   - How to handle prompt versioning?

2. **Coordinator LLM:**
   - Should it be separate model or same gpt-4-turbo?
   - How to prevent hallucination in synthesis?
   - Should it have access to raw data or just agent outputs?

3. **Data Pre-processing:**
   - How much statistical analysis before sending to LLM?
   - Should we use statistical libraries (scipy, sklearn) for pattern detection?
   - What's the token budget trade-off?

### Business
1. **User Feedback:**
   - Do we have user ratings/feedback on previous reports?
   - Can we collect feedback to improve reward functions?
   - How to define "useful" output?

2. **Scope:**
   - Which agents are most critical? (Prioritize optimization)
   - Are there new agent types we should add?
   - Should recommendations be product-specific or general?

3. **Timeline:**
   - What's the priority? (Quick wins vs long-term optimization)
   - Are there deadlines/milestones?
   - Can we do incremental rollout?

---

## Next Steps

1. **Review this approach** - Discuss and refine based on feedback
2. **Prioritize phases** - Decide what to tackle first
3. **Clarify questions** - Answer open questions above
4. **Create detailed implementation plan** - Break down into tasks
5. **Start Phase 1** - Begin with prompt improvements

---

## References

- [Agent Lightning Documentation](https://microsoft.github.io/agent-lightning/stable/)
- [Agent Lightning Paper](https://arxiv.org/abs/2508.03680)
- Current codebase: `/scripts/agents/prompt_generators/`
- Sample output: `/scripts/run_logs/1388d6/outputs/insights/agentic_insights_report.md`

---

**Document Version:** 1.0  
**Date:** 2025-11-03  
**Author:** AI Analysis  
**Status:** Draft for Review



