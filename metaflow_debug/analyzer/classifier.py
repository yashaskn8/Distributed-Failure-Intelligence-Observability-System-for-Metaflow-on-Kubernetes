from typing import List, Optional, Tuple
import re
from metaflow_debug.core.models import LogEntry, FailureCategory, FailureEvent, LogSource

class RuleBasedClassifier:
    """Detects failures and assigns a failure category based on regex and rules."""
    
    INFRA_RULES = [
        (r"ImagePullBackOff", 0.95),
        (r"ErrImagePull", 0.95),
        (r"unauthorized: authentication required", 0.8),
        (r"connection refused", 0.6)
    ]
    
    RESOURCE_RULES = [
        (r"OOMKilled", 0.95),
        (r"Exit code: 137", 0.85),
        (r"insufficient memory", 0.9)
    ]
    
    DATA_RULES = [
        (r"MetaflowDataException", 0.95),
        (r"Artifact.*not found in S3", 0.9),
        (r"Access Denied \(403\)", 0.8)
    ]
    
    APP_RULES = [
        (r"Traceback \(most recent call last\)", 0.9),
        (r"ValueError:", 0.8),
        (r"TypeError:", 0.8),
        (r"Exception:", 0.6)
    ]
    
    ORCH_RULES = [
        (r"Node failed:.*dependency", 0.8),
        (r"Failed to schedule pod", 0.7)
    ]

    def _score_category(self, logs: List[LogEntry], rules: List[Tuple[str, float]]) -> float:
        score = 0.0
        for log in logs:
            for pattern, weight in rules:
                if re.search(pattern, log.message, re.IGNORECASE):
                    # Combine static weight with dynamic anomaly score for an Advanced heuristic
                    dynamic_weight = weight + (log.anomaly_score * 0.1) 
                    score = max(score, min(1.0, dynamic_weight))
        return score

    def analyze(self, error_logs: List[LogEntry]) -> Optional[FailureEvent]:
        if not error_logs:
            return None
            
        scores = {
            FailureCategory.INFRASTRUCTURE_FAILURE: self._score_category(error_logs, self.INFRA_RULES),
            FailureCategory.RESOURCE_FAILURE: self._score_category(error_logs, self.RESOURCE_RULES),
            FailureCategory.DATA_ARTIFACT_FAILURE: self._score_category(error_logs, self.DATA_RULES),
            FailureCategory.APPLICATION_FAILURE: self._score_category(error_logs, self.APP_RULES),
            FailureCategory.ORCHESTRATION_FAILURE: self._score_category(error_logs, self.ORCH_RULES)
        }
        
        highest_cat = max(scores.items(), key=lambda x: x[1])
        if highest_cat[1] > 0.3:
            return FailureEvent(
                category=highest_cat[0],
                confidence=highest_cat[1],
                evidence=error_logs
            )
            
        # Fallback to Anomaly Base Detection
        highest_anomaly_log = max(error_logs, key=lambda l: l.anomaly_score, default=None)
        if highest_anomaly_log and highest_anomaly_log.anomaly_score > 0.5:
            return FailureEvent(
                category=FailureCategory.UNKNOWN,
                confidence=highest_anomaly_log.anomaly_score,
                evidence=[highest_anomaly_log]
            )
            
        return FailureEvent(
            category=FailureCategory.UNKNOWN,
            confidence=0.1,
            evidence=error_logs
        )

class FailureDetector:
    """Scans the timeline for error signals and delegates to the classifier."""
    def __init__(self, classifier: RuleBasedClassifier):
        self.classifier = classifier

    def extract_error_logs(self, timeline_logs: List[LogEntry]) -> List[LogEntry]:
        error_logs = []
        # Leverage both explicit errors and high anomaly scores
        for log in timeline_logs:
            if log.log_level in ["ERROR", "CRITICAL", "WARNING"]:
                error_logs.append(log)
            elif "failed" in log.message.lower() or "exit code" in log.message.lower():
                error_logs.append(log)
            elif log.anomaly_score > 0.8 and log not in error_logs:
                # Catch silent anomalies!
                error_logs.append(log)
        return error_logs
        
    def detect_failure(self, logs: List[LogEntry]) -> Optional[FailureEvent]:
        error_logs = self.extract_error_logs(logs)
        if not error_logs:
            return None
        return self.classifier.analyze(error_logs)
