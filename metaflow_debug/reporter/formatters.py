import json
from typing import Dict, Any
from metaflow_debug.core.models import AnalysisReport, FailureCategory

class ReportFormatter:
    """Formats the report to output string."""
    
    def __init__(self, report: AnalysisReport):
        self.report = report

    def to_json(self) -> str:
        return json.dumps(self.report.to_dict(), indent=2)

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.tree import Tree
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class RichCLIFormatter(ReportFormatter):
    """Rich CLI terminal integration."""
    
    def print_report(self):
        if not RICH_AVAILABLE:
            print("Rich library not installed. Falling back to JSON.")
            print(self.to_json())
            return
            
        console = Console()
        
        # STATUS HEADER
        color = "green" if self.report.status == "SUCCESS" else "red"
        status_text = Text(f"Workflow ID: {self.report.workflow_id} | Status: {self.report.status}", style=f"bold {color}")
        console.print(Panel(status_text, border_style=color))
        
        console.print(f"\n[bold]Execution Summary[/bold]")
        console.print(f"Duration: {self.report.duration_seconds:.2f} seconds")
        if self.report.trace_file_path:
            console.print(f"Distributed Trace Export: [bold blue]{self.report.trace_file_path}[/bold blue] (Drag into chrome://tracing)")
            
        if self.report.nodes:
            console.print("\n[bold]Execution DAG[/bold]")
            # Just building a visual tree from root nodes
            # To handle multiple root nodes, we find nodes with no parents
            root_nodes = [n for n in self.report.nodes.values() if not n.parent_steps]
            
            def add_children(tree: Tree, node_name: str):
                for child_node in self.report.nodes.values():
                    if node_name in child_node.parent_steps:
                        status_color = "green" if child_node.status == "Succeeded" else ("red" if child_node.status == "Failed" else "yellow")
                        child_tree = tree.add(f"[{status_color}]{child_node.name} ({child_node.status})[/{status_color}]")
                        add_children(child_tree, child_node.name)

            for root in root_nodes:
                status_color = "green" if root.status == "Succeeded" else ("red" if root.status == "Failed" else "yellow")
                t = Tree(f"[{status_color}]{root.name} ({root.status})[/{status_color}]")
                add_children(t, root.name)
                console.print(t)
                
        if self.report.failed_steps:
             console.print(f"\nFailed Steps: [red]{', '.join(self.report.failed_steps)}[/red]")
        
        if self.report.failure_event and self.report.root_cause:
             # FAILURE DETAILS
             console.print("\n[bold red]Failure Analysis[/bold red]")
             table = Table(box=box.MINIMAL_DOUBLE_HEAD)
             table.add_column("Property", style="cyan", no_wrap=True)
             table.add_column("Value")
             
             table.add_row("Category", self.report.failure_event.category.value)
             table.add_row("Confidence", f"{self.report.failure_event.confidence * 100:.1f}%")
             table.add_row("Root Cause", self.report.root_cause.explanation)
             table.add_row("Likely Factors", " • " + "\n • ".join(self.report.root_cause.ranked_causes))
             
             console.print(table)
             
             # RECOMMENDATIONS
             console.print("\n[bold green]Actionable Recommendations[/bold green]")
             for i, rec in enumerate(self.report.recommendations, 1):
                 console.print(f"  {i}. {rec}")
                 
             # LOGS Context
             console.print("\n[bold yellow]Critical Evidence Logs (with Anomaly Scores)[/bold yellow]")
             log_panel_text = ""
             for log in self.report.failure_event.evidence[-5:]: # Last 5 critical logs
                 marker = "[K8S]" if log.source.value == "kubernetes" else "[MF]"
                 score_tag = f"[TF-IDF Score: {log.anomaly_score:.2f}]" if getattr(log, 'anomaly_score', 0) > 0 else ""
                 log_panel_text += f"{marker} {log.timestamp.strftime('%H:%M:%S')} {score_tag} - {log.message}\n"
             console.print(Panel(log_panel_text.strip(), title="Correlation Trace", title_align="left"))
        else:
            console.print("\n[bold green]No failures detected. System is healthy![/bold green]")
