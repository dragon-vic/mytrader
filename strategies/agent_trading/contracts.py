from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


SCHEMAS_DIR = Path(__file__).resolve().parent / "resources" / "schemas"


# 从唯一的 JSON Schema 构建外部流程与 NT 共用的校验器。
def _validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMAS_DIR / name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


RESEARCH_VALIDATOR = _validator("research.json")
DECISION_VALIDATOR = _validator("decision.json")
