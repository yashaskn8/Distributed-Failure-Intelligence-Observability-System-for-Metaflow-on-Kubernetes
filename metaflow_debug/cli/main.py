import os
import sys

# Hack to allow running directly from CLI for testing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import argparse
from metaflow_debug.ingestion.simulator import LogCollector
from metaflow_debug.correlator.timeline import ExecutionTimeline
from metaflow_debug.correlator.tracing import ChromeTraceExporter
from metaflow_debug.analyzer.anomaly import LogAnomalyDetector
from metaflow_debug.analyzer.classifier import RuleBasedClassifier, FailureDetector
from metaflow_debug.analyzer.inference import InferenceEngine
from metaflow_debug.core.models import AnalysisReport
from metaflow_debug.reporter.formatters import RichCLIFormatter, ReportFormatter

class DebugPipeline:
    def __init__(self, run_id: str, scenario: str):
        self.run_id = run_id
        self.scenario = scenario
        
    def execute(self) -> AnalysisReport:
        # 1. Ingestion
        collector = LogCollector(self.run_id, self.scenario)
        raw_logs = collector.fetch_logs()
        
        # 2. Advanced: Heuristic Anomaly Scoring (TF-IDF)
        anomaly_detector = LogAnomalyDetector()
        anomaly_detector.fit_transform(raw_logs)

        # 3. Correlation & DAG Construction
        timeline = ExecutionTimeline(raw_logs)
        nodes = timeline.build_execution_graph()
        
        # 4. Advanced: Distributed Tracing Export
        exporter = ChromeTraceExporter(self.run_id)
        trace_file = exporter.export(nodes, f"trace_{self.run_id}.json")

        # 5. Detection & Classification
        detector = FailureDetector(RuleBasedClassifier())
        failure_event = detector.detect_failure(timeline.get_all_logs())
        
        # 6. Inference & Recommendation
        root_cause = None
        recommendations = []
        if failure_event:
            inference = InferenceEngine()
            root_cause, recommendations = inference.infer_root_cause(failure_event)
            status = "FAILED"
            failed_steps = list(set([lt.step_name for lt in failure_event.evidence if lt.step_name]))
        else:
            status = "SUCCESS"
            failed_steps = []
            
        # 7. Report Generation
        return AnalysisReport(
            workflow_id=self.run_id,
            status=status,
            duration_seconds=timeline.duration,
            failed_steps=failed_steps,
            nodes=nodes,
            failure_event=failure_event,
            root_cause=root_cause,
            recommendations=recommendations,
            context_logs=timeline.get_all_logs(),
            trace_file_path=trace_file
        )

def main():
    parser = argparse.ArgumentParser(description="Metaflow Advanced Observability System")
    parser.add_argument("--run-id", type=str, required=True, help="Workflow Run ID to analyze")
    parser.add_argument("--scenario", type=str, default="success", 
        choices=["success", "application_failure", "infrastructure_failure", "resource_failure", "data_artifact_failure"], 
        help="Simulate a specific failure scenario.")
    parser.add_argument("--format", type=str, default="cli", choices=["cli", "json"], help="Output format")
    
    args = parser.parse_args()
    
    pipeline = DebugPipeline(args.run_id, args.scenario)
    report = pipeline.execute()
    
    if args.format == "json":
        formatter = ReportFormatter(report)
        print(formatter.to_json())
    else:
        formatter = RichCLIFormatter(report)
        formatter.print_report()

if __name__ == "__main__":
    main()
