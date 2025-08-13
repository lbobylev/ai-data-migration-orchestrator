from langchain_openai import ChatOpenAI
import pytest
from typing import Any, Callable, cast

from app_types import DataMigration, MyState
from logger import get_logger
from nodes.operations_detection_node import make_operation_detection_node
import nodes.operations_detection_node as mod

logger = get_logger()


class FakeLLMResponse:
    """Stubbed LLM response for testing."""

    def __init__(self, operations: list[str]) -> None:
        self.operations = operations

    def model_dump_json(self, indent: int | None = None) -> str:
        """Simulate .model_dump_json for logging output."""
        return '{"operations": %s}' % self.operations


@pytest.fixture
def fake_llm() -> ChatOpenAI:
    """Dummy LLM object (not actually used)."""
    return cast(ChatOpenAI, object())


@pytest.fixture
def real_llm() -> ChatOpenAI:
    """Real LLM object for integration testing."""
    return ChatOpenAI(model="gpt-4o-mini", temperature=0)


@pytest.fixture
def node(fake_llm: ChatOpenAI) -> Callable[[MyState], MyState]:
    """Return the operation detection node with a stubbed LLM."""
    return make_operation_detection_node(fake_llm)


@pytest.fixture
def node_real_llm(real_llm: ChatOpenAI) -> Callable[[MyState], MyState]:
    """Return the operation detection node with a real LLM."""
    return make_operation_detection_node(real_llm)


def test_missing_user_prompt_returns_failure_status(
    node: Callable[[dict[str, Any]], dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(mod, "call_with_self_heal", lambda *a, **k: None)

    state: dict[str, Any] = {"task": DataMigration()}
    with caplog.at_level("ERROR"):
        new_state = node(state)

    assert new_state["status"] == "data_migration_classification_failed"
    assert "User input is missing" in caplog.text


def test_no_operations_detected_returns_failure(
    node: Callable[[dict[str, Any]], dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fake_call(*args: Any, **kwargs: Any) -> FakeLLMResponse:
        return FakeLLMResponse(operations=[])

    monkeypatch.setattr(mod, "call_with_self_heal", fake_call)

    state: dict[str, Any] = {"user_prompt": "delete dataset X", "task": DataMigration()}
    with caplog.at_level("ERROR"):
        new_state = node(state)

    assert new_state["status"] == "operations_detection_failed"
    assert "No operations detected" in caplog.text


def test_invalid_task_type_returns_failure(
    node: Callable[[dict[str, Any]], dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fake_call(*args: Any, **kwargs: Any) -> FakeLLMResponse:
        return FakeLLMResponse(operations=["create"])

    monkeypatch.setattr(mod, "call_with_self_heal", fake_call)

    state: dict[str, Any] = {
        "user_prompt": "create asset",
        "task": "not-a-datamigration",
    }
    with caplog.at_level("ERROR"):
        new_state = node(state)

    assert new_state["status"] == "operations_detection_failed"
    assert "No valid data migration task" in caplog.text


def test_happy_path_operations_detected_and_written(
    node: Callable[[dict[str, Any]], dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    operations: list[str] = ["create", "retrieve"]

    def fake_call(*args: Any, **kwargs: Any) -> FakeLLMResponse:
        assert len(args[0]) == 2  # Expect SystemMessage + HumanMessage
        return FakeLLMResponse(operations=operations)

    monkeypatch.setattr(mod, "call_with_self_heal", fake_call)

    dm = DataMigration()
    state: dict[str, Any] = {
        "user_prompt": "please create and then retrieve",
        "task": dm,
    }

    with caplog.at_level("INFO"):
        new_state = node(state)

    assert dm.operations == operations
    assert new_state["status"] == "operations_detected"
    assert new_state["detected_operations"] == operations
    assert new_state["task"] is dm
    assert "Detected operations:" in caplog.text
    assert '"operations":' in caplog.text


# def test_deprecation_one(
#     node_real_llm: Callable[[MyState], MyState],
#     monkeypatch: pytest.MonkeyPatch,
#     caplog: pytest.LogCaptureFixture,
# ) -> None:
#     state = MyState(
#         user_prompt="""
#   Email Thread: VIRTUS AMS: Review Supplier Library for Mirage
#   acquisition\nDate of report: 21.08.2025\nReporter: Kering Supply
#   Chain\n\nReference environment: PROD Env\n**Bug description and current
#   behavior:**\n\nWe need to:\na. Modify the \u201cSupplier Name\u201d of
#   \u201cIT01527350126\u201d from \u201cMirage spa\u201d to \u201cMirage DO NOT
#   USE\u201d\nb. Add the new supplier \u201cIT04092700121\u201d \u2013
#   \u201cMIRAGE SRL\u201d\n\n| 0                                              |
#   1                                         | 2                 | 3                     |
#   4                            | 5                            | 6                      |
#   7                  | 8                | 9                     | 10               |
#   \n|:-----------------------------------------------|:------------------------------------------|
#   :------------------|:----------------------|:-----------------------------|:-----------------------------|
#   :-----------------------|:-------------------|:-----------------|:----------------------|
#   :-----------------|\n| TO DO                                          |
#   Supplier VAT Number / Registration Number | SAP Supplier Code | Supplier
#   Country Code | Supplier Country Description | Supplier Name                |
#   Semi Finished Supplier | Supplier Type      | Supplier Status  | Catalogue
#   Uploaded By | Visibility Rules |\n| a. review the \"Supplier Name\" of
#   IT01527350126 | IT01527350126                             | 100239            |
#   IT                    | Italy                        | Mirage spa Mirage DO
#   NOT USE | No                     | Frame Manufacturer | Not Active in BC |
#   nan                   | No               |\n| b. Add a new supplier                          |
#   IT04092700121                             | 107681            | IT                    |
#   Italy                        | MIRAGE SRL                   | No                     |
#   Frame Manufacturer | Not Active in BC | nan                   | No               |
#   \n\n**Expected result:** update the supplier library ad detailed above.
#   \n\n**Notes and/or comments:**\nThe supplier \u2018Mirage spa\u2019 has been
#   acquired by a third party. As a result, the company name and VAT number have
#   been changed respectively to \u2018Mirage SRL\u2019 and
#   \u2018IT04092700121\u2019.\n\nWe therefore had to create a new master data
#   record in SAP for \u2018Mirage SRL\u2019 and disable the previous code for
#   \u2018Mirage spa\u2019.
#   """,
#         task=DataMigration(),
#     )
#
#     with caplog.at_level("INFO"):
#         new_state = node_real_llm(state)
#         ops = [x.model_dump() for x in new_state.get("detected_operations") or []]
#         logger.info(f"Detected operations:\n```json\n{ops}\n")
#
# def test_deprecation_many(
#     node_real_llm: Callable[[MyState], MyState],
#     monkeypatch: pytest.MonkeyPatch,
#     caplog: pytest.LogCaptureFixture,
# ) -> None:
#     state = MyState(
#         user_prompt="""
#   **Email Thread:** VIRTUS AMS: Review Supplier Library for LUMINA and ASTRA acquisitions  
# **Date of report:** 04.09.2025  
# **Reporter:** Kering Supply Chain  
#
# **Reference environment:** PROD Env  
#
# **Bug description and current behavior:**  
#
# We need to:  
# a. Modify the “Supplier Name” of “IT02133440987” from “Lumina s.p.a.” to “Lumina DO NOT USE”  
# b. Add the new supplier “IT05876230411” – “LUMINA SRL”  
#
# c. Modify the “Supplier Name” of “IT03398470122” from “Astra spa” to “Astra DO NOT USE”  
# d. Add the new supplier “IT07845320991” – “ASTRA SRL”  
#
# | 0   | 1 (Supplier VAT Number / Registration Number) | 2 (SAP Supplier Code) | 3 (Supplier Country Code) | 4 (Supplier Country Description) | 5 (Supplier Name) | 6 (Semi Finished Supplier) | 7 (Supplier Type) | 8 (Supplier Status) | 9 (Catalogue Uploaded By) | 10 (Visibility Rules) |  
# |:---|:----------------------------------------------|:----------------------|:--------------------------|:--------------------------------|:------------------|:----------------------------|:------------------|:--------------------|:--------------------------|:---------------------|  
# | a. review the "Supplier Name" of IT02133440987 | IT02133440987 | 109823 | IT | Italy | Lumina s.p.a. Lumina DO NOT USE | No | Frame Manufacturer | Not Active in BC | nan | No |  
# | b. Add a new supplier | IT05876230411 | 112456 | IT | Italy | LUMINA SRL | No | Frame Manufacturer | Not Active in BC | nan | No |  
# | c. review the "Supplier Name" of IT03398470122 | IT03398470122 | 110542 | IT | Italy | Astra spa Astra DO NOT USE | No | Frame Manufacturer | Not Active in BC | nan | No |  
# | d. Add a new supplier | IT07845320991 | 114877 | IT | Italy | ASTRA SRL | No | Frame Manufacturer | Not Active in BC | nan | No |  
#
# **Expected result:** update the supplier library as detailed above.  
#
# **Notes and/or comments:**  
# The suppliers *Lumina s.p.a.* and *Astra spa* have both been acquired by third parties. As a result, their company names and VAT numbers have changed respectively to *LUMINA SRL / IT05876230411* and *ASTRA SRL / IT07845320991*.  
#
# We therefore had to create new master data records in SAP for *LUMINA SRL* and *ASTRA SRL*, and disable the previous codes for *Lumina s.p.a.* and *Astra spa*.  
# """,
#         task=DataMigration(),
#     )
#
#     with caplog.at_level("INFO"):
#         new_state = node_real_llm(state)
#         ops = [x.model_dump() for x in new_state.get("detected_operations") or []]
#         logger.info(f"Detected operations:\n```json\n{ops}\n")
