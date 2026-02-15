from pathlib import Path
from typing import Any, Dict, Optional
import yaml


def load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}

    p = Path(path)

    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

