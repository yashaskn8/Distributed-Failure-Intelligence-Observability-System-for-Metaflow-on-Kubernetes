import uuid
from typing import List
from datetime import datetime, timedelta
from metaflow_debug.core.models import LogEntry, LogSource

class MockSimulator:
    """Simulates log generation for various failure scenarios."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.base_time = datetime.utcnow() - timedelta(minutes=5)
    
    def _create_log(self, source: LogSource, level: str, msg: str, step: str = None, pod: str = None, offset_sec: int = 0) -> LogEntry:
        return LogEntry(
            timestamp=self.base_time + timedelta(seconds=offset_sec),
            source=source,
            workflow_id=self.run_id,
            log_level=level,
            message=msg,
            step_name=step,
            pod_name=pod
        )

    def generate_scenario(self, scenario: str) -> List[LogEntry]:
        logs = []
        # Base setup sequence common to all scenarios
        logs.extend([
            self._create_log(LogSource.ARGO, "INFO", f"Starting workflow {self.run_id}", offset_sec=0),
            self._create_log(LogSource.ARGO, "INFO", "Scheduling node: start", step="start", offset_sec=1),
            self._create_log(LogSource.KUBERNETES, "INFO", "Pod created: start-pod-123", step="start", pod="start-pod-123", offset_sec=2),
            self._create_log(LogSource.KUBERNETES, "INFO", "Pod running: start-pod-123", step="start", pod="start-pod-123", offset_sec=5),
            self._create_log(LogSource.METAFLOW, "INFO", "Task is starting.", step="start", pod="start-pod-123", offset_sec=6),
            self._create_log(LogSource.METAFLOW, "INFO", "Initializing Datastore... S3 Backend configured.", step="start", pod="start-pod-123", offset_sec=7),
            self._create_log(LogSource.METAFLOW, "INFO", "Task completed successfully.", step="start", pod="start-pod-123", offset_sec=10),
            self._create_log(LogSource.ARGO, "INFO", "Node completed: start", step="start", offset_sec=11),
            self._create_log(LogSource.ARGO, "INFO", "Scheduling node: process_data", step="process_data", offset_sec=12),
        ])

        if scenario == "success":
            logs.extend([
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod created: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=13),
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod running: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=15),
                self._create_log(LogSource.METAFLOW, "INFO", "Task is starting.", step="process_data", pod="process-data-pod-456", offset_sec=16),
                self._create_log(LogSource.METAFLOW, "INFO", "Processing 1000 records...", step="process_data", pod="process-data-pod-456", offset_sec=17),
                self._create_log(LogSource.METAFLOW, "INFO", "Task completed successfully.", step="process_data", pod="process-data-pod-456", offset_sec=25),
                self._create_log(LogSource.ARGO, "INFO", "Workflow completed successfully", offset_sec=26),
            ])
            
        elif scenario == "application_failure":
            logs.extend([
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod created: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=13),
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod running: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=15),
                self._create_log(LogSource.METAFLOW, "INFO", "Task is starting.", step="process_data", pod="process-data-pod-456", offset_sec=16),
                self._create_log(LogSource.METAFLOW, "ERROR", "Traceback (most recent call last):", step="process_data", pod="process-data-pod-456", offset_sec=17),
                self._create_log(LogSource.METAFLOW, "ERROR", "  File \"flow.py\", line 42, in process_data", step="process_data", pod="process-data-pod-456", offset_sec=17),
                self._create_log(LogSource.METAFLOW, "ERROR", "ValueError: Invalid data format encountered in record 551", step="process_data", pod="process-data-pod-456", offset_sec=17),
                self._create_log(LogSource.KUBERNETES, "ERROR", "Container exited with non-zero exit code 1", step="process_data", pod="process-data-pod-456", offset_sec=18),
                self._create_log(LogSource.ARGO, "ERROR", "Node failed: process_data", step="process_data", offset_sec=19),
            ])

        elif scenario == "data_artifact_failure":
            logs.extend([
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod created: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=13),
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod running: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=15),
                self._create_log(LogSource.METAFLOW, "INFO", "Task is starting.", step="process_data", pod="process-data-pod-456", offset_sec=16),
                self._create_log(LogSource.METAFLOW, "INFO", "Restoring artifacts from datastore...", step="process_data", pod="process-data-pod-456", offset_sec=17),
                self._create_log(LogSource.METAFLOW, "ERROR", "metaflow.exception.MetaflowDataException: Artifact 'dataset' not found in S3 bucket. Access Denied (403).", step="process_data", pod="process-data-pod-456", offset_sec=18),
                self._create_log(LogSource.KUBERNETES, "ERROR", "Container exited with non-zero exit code 1", step="process_data", pod="process-data-pod-456", offset_sec=19),
                self._create_log(LogSource.ARGO, "ERROR", "Node failed: process_data", step="process_data", offset_sec=20),
            ])

        elif scenario == "infrastructure_failure":
            logs.extend([
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod created: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=13),
                self._create_log(LogSource.KUBERNETES, "WARNING", "Failed to pull image \"registry.local/metaflow/custom-base:v2.1\"", step="process_data", pod="process-data-pod-456", offset_sec=15),
                self._create_log(LogSource.KUBERNETES, "ERROR", "Reason: ImagePullBackOff", step="process_data", pod="process-data-pod-456", offset_sec=16),
                self._create_log(LogSource.KUBERNETES, "ERROR", "rpc error: code = Unknown desc = Error response from daemon: Get https://registry.local/v2/: unauthorized: authentication required", step="process_data", pod="process-data-pod-456", offset_sec=16),
                self._create_log(LogSource.ARGO, "ERROR", "Node failed: process_data. Reason: Pod initialization failed.", step="process_data", offset_sec=30),
            ])

        elif scenario == "resource_failure":
            logs.extend([
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod created: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=13),
                self._create_log(LogSource.KUBERNETES, "INFO", "Pod running: process-data-pod-456", step="process_data", pod="process-data-pod-456", offset_sec=15),
                self._create_log(LogSource.METAFLOW, "INFO", "Task is starting. Loading large dataset into pandas...", step="process_data", pod="process-data-pod-456", offset_sec=16),
                self._create_log(LogSource.KUBERNETES, "ERROR", "Reason: OOMKilled", step="process_data", pod="process-data-pod-456", offset_sec=20),
                self._create_log(LogSource.KUBERNETES, "ERROR", "Pod exited. Memory usage: 2048Mi / Limit: 2048Mi", step="process_data", pod="process-data-pod-456", offset_sec=20),
                self._create_log(LogSource.ARGO, "ERROR", "Node failed: process_data. Exit code: 137", step="process_data", offset_sec=21),
            ])

        else:
            raise ValueError(f"Unknown scenario: {scenario}")

        return logs

class LogCollector:
    def __init__(self, run_id: str, scenario: str = "success"):
        self.run_id = run_id
        self.scenario = scenario
        
    def fetch_logs(self) -> List[LogEntry]:
        simulator = MockSimulator(self.run_id)
        return simulator.generate_scenario(self.scenario)
