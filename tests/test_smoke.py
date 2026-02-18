import os
from presentation import cli


def test_cli_main_smoke(tmp_path, monkeypatch):
    csv_path = os.path.join(os.path.dirname(__file__), "fixtures", "transactions.csv")
    out = tmp_path / "parquet"
    monkeypatch.setenv("DATA_SOURCE_PATH", csv_path)
    monkeypatch.setenv("OUTPUT_DIR", str(out))
    cli.main()
    assert out.exists()
