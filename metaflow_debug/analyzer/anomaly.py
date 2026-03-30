import math
import string
from typing import List, Dict
from metaflow_debug.core.models import LogEntry

class LogAnomalyDetector:
    """
    Advanced Heuristic: Computes a baseline TF-IDF/Entropy score for logs within the run.
    Logs that are highly unique or structurally anomalous get a higher anomaly_score.
    This helps surface the "needle in the haystack" when the regex rule engine fails.
    """
    
    def __init__(self):
        self.doc_frequencies: Dict[str, int] = {}
        self.total_docs = 0
        
    def _tokenize(self, text: str) -> set:
        # Strip punctuation and numbers to find structural log patterns
        translator = str.maketrans('', '', string.punctuation + string.digits)
        clean = text.translate(translator).lower()
        return set(clean.split())

    def fit_transform(self, logs: List[LogEntry]):
        self.total_docs = len(logs)
        if self.total_docs == 0:
            return

        # 1. Build document frequencies
        for log in logs:
            tokens = self._tokenize(log.message)
            for token in tokens:
                self.doc_frequencies[token] = self.doc_frequencies.get(token, 0) + 1
                
        # 2. Score logs: If a log contains rare words, it's anomalous
        for log in logs:
            tokens = self._tokenize(log.message)
            if not tokens:
                continue
                
            score = 0.0
            for token in tokens:
                idf = math.log(self.total_docs / (1 + self.doc_frequencies[token]))
                score += idf
                
            # Normalize and amplify error logs inherently
            base_score = score / len(tokens)
            if log.log_level in ["ERROR", "CRITICAL", "WARNING"]:
                base_score *= 2.0
                
            log.anomaly_score = round(min(1.0, base_score / 5.0), 3)  # Normalize roughly between 0-1
