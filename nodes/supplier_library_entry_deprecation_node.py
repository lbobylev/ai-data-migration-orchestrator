import json
from typing import Any, Dict, List
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from enrichers import ENRICHERS

from app_types import (
    AssetPatch,
    Environment,
    ExecutionTask,
    MyState,
)
from llm_utils import call_with_self_heal
from logger import get_logger
from langchain.prompts import PromptTemplate

from operation_helpers import create_enriched_patches
from asset_spec import ASSET_SPECS

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


class Deprecation(BaseModel):
    create_data: Dict[str, str]
    update_data: Dict[str, str]
    organization_patch: Dict[str, Any]


class Response(BaseModel):
    results: List[Deprecation]


DATA_EXTRACTION_PROMPT = """
Given the task description and the input data, your task is to return a valid JSON with the following structure:

{
    "results": [
        {
            "update_data": <library entry data to update>,   # Only one record allowed
            "create_data": <library entry data to create>,   # Only one record allowed
            "organization_patch": {
                "predicate": {
                    "attributes": {
                        "vatCode": "<previous VAT code>",
                        "sapCode": "<previous SAP code>"
                    }
                },
                "patch": {
                    "attributes": {
                        "vatCode": "<new VAT code>",
                        "sapCode": "<new SAP code>"
                    }
                }
            }
        },
        ...
    ]
}

Rules:
1. Determine which VAT and SAP codes need to be updated based on the input data.
2. There must always be a unique mapping between previous and new codes.
3. No duplicates are allowed.
4. Ensure the output is valid JSON.
5. If many deprecations are found, return them all in the results array.
"""

USER_PROMPT_TEMPLATE = """
Task description: {task_description}
Input data: {input_data}
"""


def supplier_library_entry_deprecation_node(state: MyState) -> MyState:
    logger.info("Starting SupplierLibraryEntry deprecation node.")
    raise RuntimeError("Not implemented yet")

    data = state.get("data") or []

    messages = [
        SystemMessage(content=DATA_EXTRACTION_PROMPT),
        HumanMessage(
            content=PromptTemplate.from_template(USER_PROMPT_TEMPLATE).format(
                input_data=json.dumps(data),
                task_description=state.get("user_input"),
            )
        ),
    ]

    results = call_with_self_heal(llm, messages, Response).results
    xs = [x.model_dump() for x in results]
    logger.info(f"Deprecation results:\n```json\n{json.dumps(xs, indent=2)}\n```")

    task_description = state.get("user_input") or ""
    environments = state.get("environments") or []

    enrich = ENRICHERS.get(("SupplierLibraryEntry", "deprecation"))
    if enrich is None:
        logger.error("No enricher found for SupplierLibraryEntry deprecation")
        return {"status": "operation_processing_failed"}

    asset_spec = ASSET_SPECS["SupplierLibraryEntry"]
    tasks: List[ExecutionTask] = []
    for deprecation in results:

        # deprecate old entry
        # SupplierLibraryEntry update

        update_op_patches = create_enriched_patches(
            llm,
            asset_type="SupplierLibraryEntry",
            operation_name="update",
            data=[deprecation.update_data],
            task_description=task_description,
            environments=environments,
        )

        # create new library entry
        # SupplierLibraryEntry create

        create_op_patches = create_enriched_patches(
            llm,
            asset_type="SupplierLibraryEntry",
            operation_name="create",
            data=[deprecation.create_data],
            task_description=task_description,
            environments=environments,
        )

        # update organization vatCode and sapCode
        # Organization update

        patch = AssetPatch(
            predicate=deprecation.organization_patch["predicate"],
            patch=deprecation.organization_patch["patch"],
        )
        patches: Dict[Environment, List[AssetPatch]] = {
            env: [patch] for env in environments
        }

    return {"status": "operation_processed"}
