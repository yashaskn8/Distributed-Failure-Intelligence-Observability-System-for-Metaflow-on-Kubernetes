import json
from typing import List, Dict
from datetime import datetime
from metaflow_debug.core.models import LogEntry, StepNode

class ChromeTraceExporter:
    """
    Exports the execution timeline and DAG to a Chrome Tracing format (trace.json).
    This allows developers to drag-and-drop the trace into chrome://tracing or Perfetto 
    for visual distributed tracing of their Metaflow ML pipelines.
    """
    
    def __init__(self, run_id: str):
        self.run_id = run_id
        
    def export(self, nodes: Dict[str, StepNode], output_path: str):
        trace_events = []
        
        # We need a base time to convert to microseconds
        base_time = None
        for name, node in nodes.items():
            if base_time is None or node.start_time < base_time:
                base_time = node.start_time
                
        if not base_time:
            return None
            
        # Add metadata
        trace_events.append({
            "name": "process_name",
            "ph": "M",
            "pid": 1,
            "args": {"name": f"Metaflow Run: {self.run_id}"}
        })
        
        # Add Steps as B/E (Begin/End) events
        for name, node in nodes.items():
            if not node.end_time:
                continue
                
            start_us = int((node.start_time - base_time).total_seconds() * 1e6)
            end_us = int((node.end_time - base_time).total_seconds() * 1e6)
            dur_us = end_us - start_us
            
            # Argo Layer Span
            trace_events.append({
                "name": f"Argo Node: {name}",
                "cat": "orchestration",
                "ph": "X",
                "ts": start_us,
                "dur": dur_us,
                "pid": 1,
                "tid": 1,
                "args": {"status": node.status, "pod": node.pod_name}
            })
            
            # Pod Layer Span (Starts slightly after Argo schedules it)
            trace_events.append({
                "name": f"K8s Pod: {node.pod_name}",
                "cat": "infrastructure",
                "ph": "X",
                "ts": start_us + 1000000, # 1 sec offset for pod init
                "dur": max(0, dur_us - 1000000),
                "pid": 1,
                "tid": 2,
                "args": {"status": node.status}
            })

            # Metaflow Layer Span
            color = "good" if node.status == "Succeeded" else "terrible"
            trace_events.append({
                "name": f"Metaflow Step: {name}",
                "cat": "application",
                "ph": "X",
                "ts": start_us + 2000000,
                "dur": max(0, dur_us - 2000000),
                "pid": 1,
                "tid": 3,
                "cname": color,
                "args": {"step": name}
            })

        try:
            with open(output_path, "w") as f:
                json.dump(trace_events, f, indent=2)
            return output_path
        except Exception as e:
            print(f"Failed to write trace: {e}")
            return None
