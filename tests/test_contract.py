"""领域资料与契约的一致性校验。"""

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def load(name):
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


def test_fixture_conforms_to_schema():
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(load("fixtures/context.json"),
                        load("contracts/context.schema.json"))


def test_reference_data_time_windows_parse():
    from src.context import load_full_context
    from src.evidence.reference import ReferenceData

    ref = ReferenceData.from_context(load_full_context(ROOT / "fixtures" / "context.json"))
    assert ref.stage_names() == ["self_check", "rectification", "review"]
    assert set(ref.permission_matrix) == {
        "false_report", "perfunctory_rectification", "review_failed",
        "suspected_crime",
    }
    # 涉嫌犯罪移送必须至少两类审批角色，保证相互制约
    assert len(ref.permission_matrix["suspected_crime"]["approve_roles"]) >= 2
