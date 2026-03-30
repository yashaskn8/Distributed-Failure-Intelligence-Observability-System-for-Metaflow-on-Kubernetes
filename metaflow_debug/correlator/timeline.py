from typing import List, Dict, Optional
from datetime import datetime
from metaflow_debug.core.models import LogEntry, StepNode

class ExecutionTimeline:
    """Combines logs into an ordered execution trace with context boundaries."""
    
    def __init__(self, logs: List[LogEntry]):
        self.chronological_logs = sorted(logs, key=lambda x: x.timestamp)
        self.duration = 0.0
        if self.chronological_logs:
            self.duration = (self.chronological_logs[-1].timestamp - self.chronological_logs[0].timestamp).total_seconds()
            
    def get_logs_by_step(self) -> Dict[str, List[LogEntry]]:
        grouped = {}
        for log in self.chronological_logs:
            step = log.step_name or "system_level"
            if step not in grouped:
                grouped[step] = []
            grouped[step].append(log)
        return grouped
        
    def get_all_logs(self) -> List[LogEntry]:
        return self.chronological_logs

    def build_execution_graph(self) -> Dict[str, StepNode]:
        """Infers the Argo / Metaflow DAG from the logs."""
        nodes: Dict[str, StepNode] = {}
        for log in self.chronological_logs:
            if not log.step_name:
                continue
                
            step = log.step_name
            if step not in nodes:
                nodes[step] = StepNode(name=step, status="Pending", start_time=log.timestamp)
                
            # Update Pod state
            if log.pod_name:
                nodes[step].pod_name = log.pod_name
                
            # State Management
            msg = log.message.lower()
            if "completed successfully" in msg or "node completed" in msg:
                nodes[step].status = "Succeeded"
                nodes[step].end_time = log.timestamp
            elif "failed" in msg or "error" in msg:
                if nodes[step].status != "Succeeded": # Prevent overwriting success if a background error happens
                    nodes[step].status = "Failed"
                    nodes[step].end_time = log.timestamp
                    
            # Basic Parent inference (Heuristic: previous step completed recently)
            # In a real system, this comes from Argo Workflow DAG spec directly.
            if len(nodes) > 1 and not nodes[step].parent_steps:
                prev_steps = [n for n in nodes.values() if n.name != step and n.status == "Succeeded"]
                if prev_steps:
                    nodes[step].parent_steps.append(prev_steps[-1].name)
                    
        return nodes
