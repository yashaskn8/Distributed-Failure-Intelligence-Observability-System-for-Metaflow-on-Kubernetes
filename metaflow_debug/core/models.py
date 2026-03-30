import sys
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime

class LogSource(str, Enum):
    KUBERNETES = "kubernetes"
    ARGO = "argo"
    METAFLOW = "metaflow"
    SYSTEM = "system"

class FailureCategory(str, Enum):
    INFRASTRUCTURE_FAILURE = "INFRASTRUCTURE_FAILURE"
    ORCHESTRATION_FAILURE = "ORCHESTRATION_FAILURE"
    APPLICATION_FAILURE = "APPLICATION_FAILURE"
    RESOURCE_FAILURE = "RESOURCE_FAILURE"
    CONFIGURATION_FAILURE = "CONFIGURATION_FAILURE"
    DATA_ARTIFACT_FAILURE = "DATA_ARTIFACT_FAILURE"
    UNKNOWN = "UNKNOWN"

@dataclass
class LogEntry:
    timestamp: datetime
    source: LogSource
    workflow_id: str
    log_level: str
    message: str
    step_name: Optional[str] = None
    pod_name: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    anomaly_score: float = 0.0  # Added for Anomaly Detection Engine

@dataclass
class StepNode:
    name: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    pod_name: Optional[str] = None
    parent_steps: List[str] = field(default_factory=list)

@dataclass
class FailureEvent:
    category: FailureCategory
    confidence: float
    evidence: List[LogEntry]

@dataclass
class RootCause:
    explanation: str
    ranked_causes: List[str]

@dataclass
class AnalysisReport:
    workflow_id: str
    status: str
    duration_seconds: float
    failed_steps: List[str]
    nodes: Dict[str, StepNode]
    failure_event: Optional[FailureEvent]
    root_cause: Optional[RootCause]
    recommendations: List[str]
    context_logs: List[LogEntry]
    trace_file_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "status": self.status,
            "duration_seconds": self.duration_seconds,
            "failed_steps": self.failed_steps,
            "trace_file": self.trace_file_path,
            "failure_event": {
                "category": self.failure_event.category.value,
                "confidence": self.failure_event.confidence,
                "evidence": [f"[{e.anomaly_score:.2f}] {e.message}" for e in self.failure_event.evidence]
            } if self.failure_event else None,
            "root_cause": {
                "explanation": self.root_cause.explanation,
                "ranked_causes": self.root_cause.ranked_causes
            } if self.root_cause else None,
            "recommendations": self.recommendations,
        }
