#!/usr/bin/env python3
"""
Prompt Optimization Script
Version: 1.0.0

Script to run prompt optimization for agents (Phase 3).
"""

import os
import sys
import argparse
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.prompt_optimization import Phase3Integration, TrainingDataLoader
from agents.prompt_generators import (
    DailyMetricsPromptGenerator,
    UserSegmentationPromptGenerator,
    GeographicPromptGenerator,
    CohortRetentionPromptGenerator,
    RevenueOptimizationPromptGenerator,
    DataQualityPromptGenerator
)

def get_prompt_generator(agent_type: str):
    """Get prompt generator for agent type."""
    generators = {
        'daily_metrics': DailyMetricsPromptGenerator(),
        'user_segmentation': UserSegmentationPromptGenerator(),
        'geographic': GeographicPromptGenerator(),
        'cohort_retention': CohortRetentionPromptGenerator(),
        'revenue_optimization': RevenueOptimizationPromptGenerator(),
        'data_quality': DataQualityPromptGenerator()
    }
    return generators.get(agent_type)

def main():
    parser = argparse.ArgumentParser(description='Optimize prompts for agents (Phase 3)')
    parser.add_argument('--agent-type', required=True, 
                       choices=['daily_metrics', 'user_segmentation', 'geographic', 
                               'cohort_retention', 'revenue_optimization', 'data_quality'],
                       help='Agent type to optimize')
    parser.add_argument('--steps', type=int, default=50, 
                       help='Number of optimization steps (default: 50)')
    parser.add_argument('--run-logs-dir', default='run_logs',
                       help='Directory containing run logs for training data')
    
    args = parser.parse_args()
    
    # Enable Phase 3
    os.environ['ENABLE_PHASE3_OPTIMIZATION'] = '1'
    
    print(f"🚀 Starting prompt optimization for {args.agent_type}", file=sys.stderr)
    print(f"📊 Optimization steps: {args.steps}", file=sys.stderr)
    
    # Get current prompts from prompt generator
    prompt_generator = get_prompt_generator(args.agent_type)
    if not prompt_generator:
        print(f"❌ Unknown agent type: {args.agent_type}", file=sys.stderr)
        return 1
    
    # Get current system prompt
    current_system_prompt = prompt_generator.get_system_prompt()
    
    # Create a sample user prompt template (would normally come from generate_prompt)
    current_prompts = {
        'system_prompt': current_system_prompt,
        'user_prompt_template': 'Analysis instructions template'
    }
    
    # Initialize Phase 3 integration
    phase3 = Phase3Integration(enable_optimization=True)
    
    # Load training data
    data_loader = TrainingDataLoader(run_logs_dir=args.run_logs_dir)
    training_data = data_loader.get_training_data_for_agent(args.agent_type, limit=20)
    
    if not training_data:
        print(f"⚠️ No training data found. Need at least one historical run.", file=sys.stderr)
        print(f"   Run some agents first to generate training data.", file=sys.stderr)
        return 1
    
    print(f"📈 Loaded {len(training_data)} training samples", file=sys.stderr)
    
    # Run optimization
    from agents.prompt_optimization import APOPromptOptimizer
    
    optimizer = APOPromptOptimizer(args.agent_type, use_agent_lightning=False)
    
    result = optimizer.optimize_prompts(
        current_prompts,
        training_data,
        optimization_steps=args.steps
    )
    
    if result:
        print(f"\n✅ Optimization complete!", file=sys.stderr)
        print(f"   Initial reward: {result['initial_reward']:.3f}", file=sys.stderr)
        print(f"   Best reward: {result['best_reward']:.3f}", file=sys.stderr)
        print(f"   Improvement: {result['improvement']:.3f}", file=sys.stderr)
        print(f"   Best version: {result['best_version']}", file=sys.stderr)
        return 0
    else:
        print(f"❌ Optimization failed", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())

