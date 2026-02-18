from typing import Mapping, Any
from core.interfaces import HealthMonitor

try:  # pragma: no cover
    import mlflow
except ImportError:  # pragma: no cover
    mlflow = None


class MflowMonitor(HealthMonitor):
    def __init__(self, experiment: str, run_name: str) -> None:
        self._experiment = experiment
        self._run_name = run_name

    def report_metrics(self, metrics: Mapping[str, Any]) -> None:
        if mlflow is None:
            return

        mlflow.set_experiment(self._experiment)
        with mlflow.start_run(run_name=self._run_name):
            mlflow.log_metrics(
                {k: float(v) for k, v in metrics.items() if isinstance(v, (int, float))}
            )
