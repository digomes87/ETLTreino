from pathlib import Path
from pyspark.sql import DataFrame
from core.interfaces import Reporter


class DeltaReporter(Reporter):
    def __init__(self, output_dir: str) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").save(str(self._output_dir))
