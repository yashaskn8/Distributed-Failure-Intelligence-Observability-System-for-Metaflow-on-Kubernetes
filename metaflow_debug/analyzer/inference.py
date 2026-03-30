from typing import List, Tuple
import re
from metaflow_debug.core.models import FailureEvent, FailureCategory, RootCause

class InferenceEngine:
    """Infers the root cause explanation and actionable recommendations."""
    
    def infer_root_cause(self, failure_event: FailureEvent) -> Tuple[RootCause, List[str]]:
        cat = failure_event.category
        evidence_text = "\n".join(e.message for e in failure_event.evidence)
        
        explanation = "Unknown failure."
        ranked_causes = []
        recs = []
        
        if cat == FailureCategory.INFRASTRUCTURE_FAILURE:
            if "ImagePullBackOff" in evidence_text or "ErrImagePull" in evidence_text:
                explanation = "Kubernetes failed to pull the Docker image required for the step."
                if "unauthorized" in evidence_text:
                    ranked_causes = ["Docker registry credentials missing or expired", "Incorrect image path/tag"]
                    recs = [
                        "Verify ImagePullSecrets in your Argo Workflow or K8s ServiceAccount.",
                        "Check if the container image requires VPN or internal registry access."
                    ]
                else:
                    ranked_causes = ["Image tag does not exist", "Registry network failure"]
                    recs = [
                        "Verify the docker image name and tag are correct.",
                        "Check Kubernetes cluster network connectivity."
                    ]
        
        elif cat == FailureCategory.RESOURCE_FAILURE:
            if "OOMKilled" in evidence_text or "137" in evidence_text:
                explanation = "The container exceeded its allocated memory limit and was killed by Linux OOM killer."
                ranked_causes = ["Data payload too large for pandas/memory footprint", "Memory leak in user code", "Insufficient pod memory limit"]
                recs = [
                    "Increase the @resources(memory=...) annotation on your Metaflow step.",
                    "Review code for large in-memory data structures (consider pagination/chunking)."
                ]
        
        elif cat == FailureCategory.DATA_ARTIFACT_FAILURE:
            explanation = "Cross-step Data Artifact deserialization or access failed from Datastore."
            ranked_causes = ["S3/GCS bucket IAM permission error", "Artifact garbage collection", "Network partition to datastore"]
            recs = [
                "Verify Kubernetes pod IAM roles can read from the central Metaflow S3/GCS bucket.",
                "Ensure artifacts weren't manually deleted from the datastore backend."
            ]
        
        elif cat == FailureCategory.APPLICATION_FAILURE:
            explanation = "A Python exception occurred in the user-defined Metaflow step code."
            # Attempt to extract exception name
            match = re.search(r"(\w+Error|Exception):\s*(.*)", evidence_text)
            if match:
                ranked_causes = [f"Code raised {match.group(1)}: {match.group(2)}"]
                recs = [
                    f"Investigate the application logic where {match.group(1)} is thrown.",
                    "Check the preceding logs for context on the data state."
                ]
            else:
                ranked_causes = ["Unhandled exception in step execution."]
                recs = ["Check stack traces in the Metaflow user code."]
                
        elif cat == FailureCategory.ORCHESTRATION_FAILURE:
            explanation = "Argo Workflow engine failed to progress the DAG."
            ranked_causes = ["Failed validation", "Missing dependencies"]
            recs = ["Check the Argo workflow manifest and step dependencies."]
            
        return RootCause(explanation=explanation, ranked_causes=ranked_causes), recs
