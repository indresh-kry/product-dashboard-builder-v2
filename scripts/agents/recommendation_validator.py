#!/usr/bin/env python3
"""
Recommendation Validator
Version: 1.0.0
Last Updated: 2025-11-03

Validates and filters recommendations based on quality criteria.
Filters out generic, low-quality recommendations.
"""

import re
from typing import Dict, Any, List, Optional


class RecommendationValidator:
    """Validates recommendations and filters out low-quality ones."""
    
    # Generic phrases that indicate vague recommendations
    GENERIC_PHRASES = [
        r'\bimprove\b',
        r'\benhance\b',
        r'\boptimize\b',
        r'\bimplement strategies?\b',
        r'\bdeploy models?\b',
        r'\buse (AI|ML|machine learning|artificial intelligence)\b',
        r'\bbuild features?\b',
        r'\badd functionality\b',
        r'\bdevelop.*strateg(?:y|ies)\b',
        r'\bcreate.*campaigns?\b',
        r'\bintroduce.*elements?\b',
        r'\banalyze.*reasons?\b',
        r'\bfocus on\b',
        r'\bconsider\b',
        r'\bexplore\b',
        r'\binvestigate\b',
    ]
    
    # Specific indicators of actionable recommendations
    SPECIFIC_INDICATORS = [
        r'\bday \d+\b',  # Specific days
        r'\b\d+%\b',  # Percentages
        r'\$\d+(?:\.\d+)?\b',  # Dollar amounts
        r'\b\d+ (?:users?|days?|weeks?|hours?)\b',  # Specific numbers
        r'\bcohort \d{4}-\d{2}-\d{2}\b',  # Specific dates
        r'\bfrom \S+ to \S+\b',  # Value changes
        r'\bwithin \d+ (?:days?|weeks?)\b',  # Timeframes
        r'\btest with \d+\b',  # Test sizes
        r'\breplicate\b',  # Action verbs
        r'\bimplement\b',  # Action verbs
        r'\bset up\b',  # Action phrases
        r'\btrigger.*on\b',  # Specific triggers
    ]
    
    def __init__(self, min_specificity_score: float = 6.0, min_actionability_score: float = 5.5, strict_mode: bool = True):
        """
        Initialize validator.
        
        Args:
            min_specificity_score: Minimum specificity score (0-10) to keep recommendation
            min_actionability_score: Minimum actionability score (0-10) to keep recommendation
            strict_mode: If True, use stricter filtering thresholds for better quality
        """
        # If strict mode enabled, use higher thresholds
        if strict_mode:
            self.min_specificity_score = max(min_specificity_score, 6.5)
            self.min_actionability_score = max(min_actionability_score, 6.0)
        else:
            self.min_specificity_score = min_specificity_score
            self.min_actionability_score = min_actionability_score
        
        self.strict_mode = strict_mode
        self.generic_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.GENERIC_PHRASES]
        self.specific_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.SPECIFIC_INDICATORS]
    
    def validate_recommendation(self, recommendation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single recommendation.
        
        Args:
            recommendation: Recommendation dict with 'action' and 'evidence' fields
            
        Returns:
            Dict with validation results:
            {
                'is_valid': bool,
                'specificity_score': float (0-10),
                'actionability_score': float (0-10),
                'data_support_score': float (0-10),
                'total_score': float (0-10),
                'issues': List[str],
                'should_filter': bool
            }
        """
        action = str(recommendation.get('action', ''))
        evidence = str(recommendation.get('evidence', ''))
        
        # Calculate scores
        specificity_score = self._calculate_specificity_score(action, evidence)
        actionability_score = self._calculate_actionability_score(action)
        data_support_score = self._calculate_data_support_score(evidence)
        
        # Weighted total score
        total_score = (
            specificity_score * 0.4 +
            actionability_score * 0.4 +
            data_support_score * 0.2
        )
        
        # Collect issues
        issues = []
        
        if specificity_score < self.min_specificity_score:
            issues.append(f"Low specificity (score: {specificity_score:.1f})")
        
        if actionability_score < self.min_actionability_score:
            issues.append(f"Low actionability (score: {actionability_score:.1f})")
        
        if self._has_generic_phrases(action):
            issues.append("Contains generic phrases")
        
        if not self._has_specific_indicators(action):
            issues.append("Lacks specific indicators (dates, numbers, metrics)")
        
        if not evidence or len(evidence.strip()) < 20:
            issues.append("Evidence is too short or missing")
        
        if not self._references_data_in_evidence(evidence):
            issues.append("Evidence does not reference specific data points")
        
        # Determine if should filter (strict mode uses higher thresholds)
        if self.strict_mode:
            should_filter = (
                total_score < 6.0 or  # Higher threshold for strict mode
                (specificity_score < self.min_specificity_score or actionability_score < self.min_actionability_score) or  # Either must pass
                (self._has_generic_phrases(action) and total_score < 5.5)  # Filter generic more aggressively
            )
        else:
            should_filter = (
                total_score < 5.0 or  # Lower threshold for lenient mode
                (specificity_score < self.min_specificity_score and actionability_score < self.min_actionability_score) or  # Both must be low
                (self._has_generic_phrases(action) and total_score < 4.5)  # Only filter generic if very low score
            )
        
        return {
            'is_valid': not should_filter,
            'specificity_score': round(specificity_score, 2),
            'actionability_score': round(actionability_score, 2),
            'data_support_score': round(data_support_score, 2),
            'total_score': round(total_score, 2),
            'issues': issues,
            'should_filter': should_filter
        }
    
    def _calculate_specificity_score(self, action: str, evidence: str) -> float:
        """Calculate specificity score (0-10)."""
        score = 6.0  # Higher base score (was 5.0)
        
        # Deduct for generic phrases (less harsh)
        generic_count = sum(1 for pattern in self.generic_patterns if pattern.search(action))
        score -= generic_count * 1.0  # Less harsh: was 1.5
        
        # Add for specific indicators
        specific_count = sum(1 for pattern in self.specific_patterns if pattern.search(action))
        score += specific_count * 0.8  # Slightly reduced: was 1.0
        
        # Add for evidence containing data
        if self._references_data_in_evidence(evidence):
            score += 1.0  # Reduced: was 1.5
        
        # Add for metric references
        if re.search(r'\b(?:metric|ARPU|DAU|retention|revenue|conversion)\b', action, re.IGNORECASE):
            score += 0.5  # Reduced: was 1.0
        
        # Add for date/cohort references
        if re.search(r'\b(?:cohort|date|day \d+|202\d-\d{2}-\d{2})\b', action, re.IGNORECASE):
            score += 0.5  # Reduced: was 1.0
        
        # Add for percentage or dollar amounts
        if re.search(r'\b\d+%|\$\d+', action):
            score += 0.5  # Reduced: was 1.0
        
        return min(10.0, max(0.0, score))
    
    def _calculate_actionability_score(self, action: str) -> float:
        """Calculate actionability score (0-10)."""
        score = 5.5  # Higher base score (was 5.0)
        
        # Action verbs boost actionability
        action_verbs = [
            'implement', 'set up', 'replicate', 'test', 'trigger', 'deploy',
            'create', 'build', 'add', 'remove', 'change', 'update', 'modify',
            'launch', 'schedule', 'initiate', 'execute', 'run', 'start'
        ]
        action_verb_count = sum(1 for verb in action_verbs if verb.lower() in action.lower())
        score += action_verb_count * 0.8  # Reduced: was 1.0
        
        # Specific timeframes
        if re.search(r'\b(?:within|by|on|starting) \d+ (?:days?|weeks?|hours?)\b', action, re.IGNORECASE):
            score += 1.5  # Reduced: was 2.0
        
        # Specific targets
        if re.search(r'\b(?:target|test with|for) \d+', action, re.IGNORECASE):
            score += 1.0  # Reduced: was 1.5
        
        # Vague phrases reduce actionability (less harsh)
        vague_phrases = ['focus on', 'consider', 'explore', 'investigate', 'analyze']
        vague_count = sum(1 for phrase in vague_phrases if phrase.lower() in action.lower())
        score -= vague_count * 1.0  # Less harsh: was 1.5
        
        return min(10.0, max(0.0, score))
    
    def _calculate_data_support_score(self, evidence: str) -> float:
        """Calculate data support score (0-10)."""
        if not evidence or len(evidence.strip()) < 10:
            return 0.0
        
        score = 3.0  # Base score for having evidence
        
        # Numbers/percentages in evidence
        number_count = len(re.findall(r'\b\d+(?:\.\d+)?%?\b', evidence))
        score += min(3.0, number_count * 0.5)
        
        # Metric names
        metric_keywords = ['ARPU', 'DAU', 'retention', 'revenue', 'conversion', 'churn', 'cohort']
        metric_count = sum(1 for metric in metric_keywords if metric.lower() in evidence.lower())
        score += min(2.0, metric_count * 0.5)
        
        # Date/cohort references
        if re.search(r'\b(?:202\d-\d{2}-\d{2}|cohort|date)\b', evidence):
            score += 1.5
        
        # Comparisons
        if re.search(r'\b(?:vs|compared to|versus|above|below|higher|lower)\b', evidence, re.IGNORECASE):
            score += 1.0
        
        return min(10.0, max(0.0, score))
    
    def _has_generic_phrases(self, text: str) -> bool:
        """Check if text contains generic phrases."""
        return any(pattern.search(text) for pattern in self.generic_patterns)
    
    def _has_specific_indicators(self, text: str) -> bool:
        """Check if text has specific indicators."""
        return any(pattern.search(text) for pattern in self.specific_patterns)
    
    def _references_data_in_evidence(self, evidence: str) -> bool:
        """Check if evidence references specific data points."""
        if not evidence:
            return False
        
        # Check for numbers
        has_numbers = bool(re.search(r'\b\d+(?:\.\d+)?%?\b', evidence))
        
        # Check for metric names
        has_metrics = bool(re.search(r'\b(?:ARPU|DAU|retention|revenue|conversion|churn|metric)\b', evidence, re.IGNORECASE))
        
        # Check for dates/cohorts
        has_dates = bool(re.search(r'\b(?:202\d-\d{2}-\d{2}|cohort|date)\b', evidence))
        
        return has_numbers or has_metrics or has_dates
    
    def filter_recommendations(self, recommendations: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Filter recommendations, keeping only high-quality ones.
        
        Args:
            recommendations: List of recommendation dicts
            
        Returns:
            Tuple of (filtered_recommendations, filtered_out_recommendations)
        """
        filtered = []
        filtered_out = []
        
        for rec in recommendations:
            validation = self.validate_recommendation(rec)
            
            # Add validation scores to recommendation
            rec['validation'] = {
                'specificity_score': validation['specificity_score'],
                'actionability_score': validation['actionability_score'],
                'data_support_score': validation['data_support_score'],
                'total_score': validation['total_score']
            }
            
            if validation['should_filter']:
                rec['filtered_reason'] = validation['issues']
                filtered_out.append(rec)
            else:
                filtered.append(rec)
        
        return filtered, filtered_out
    
    def validate_insights(self, insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate and filter insights that have recommendations.
        
        Args:
            insights: List of insight dicts
            
        Returns:
            List of validated insights (with scores added)
        """
        validated = []
        
        for insight in insights:
            # If insight has a recommendation field, validate it
            if 'recommendation' in insight:
                recommendation = {
                    'action': insight.get('recommendation', ''),
                    'evidence': insight.get('evidence', '')
                }
                validation = self.validate_recommendation(recommendation)
                
                insight['validation'] = {
                    'specificity_score': validation['specificity_score'],
                    'actionability_score': validation['actionability_score'],
                    'data_support_score': validation['data_support_score'],
                    'total_score': validation['total_score']
                }
            
            validated.append(insight)
        
        return validated

