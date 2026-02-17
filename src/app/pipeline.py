from dataclasses import dataclass
from time import perf_counter
from typing import Optional, Mapping, Any
from core.interfaces import DataReader, Transformer, Validator, Reporter, HealthMonitor


@dataclass
class Pipeline:
    reader: DataReader
    transformer: Transformer
    validator: Validator
    reporter: Reporter
    monitor: Optional[HealthMonitor] = None

    def run(self) -> Mapping[str, Any]:
        t0 = perf_counter()
        raw = self.reader.read()
        t1 = perf_counter()

        transformed = self.transformer.transform(raw)
        t2 = perf_counter()

        errors = list(self.validator.validate(transformed))
        t3 = perf_counter()

        if errors:
            raise ValueError(f"Data quality errors: {errors}")

        self.reporter.write(transformed)
        t4 = perf_counter()

        metrics = {
            "rows_raw": raw.count(),
            "rows_transformed": transformed.count(),
            "read_s": round(t1 - t0, 6),
            "transform_s": round(t2 - t1, 6),
            "validate_s": round(t3 - t2, 6),
            "report_s": round(t4 - t3, 6),
            "total_s": round(t4 - t0, 6),
        }
        if self.monitor:
            self.monitor.report_metrics(metrics)
        return metrics
