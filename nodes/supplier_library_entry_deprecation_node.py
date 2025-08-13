from datetime import datetime
import json
from typing import Any, Dict, List
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from enrichers import ENRICHERS

from app_types import (
    AssetOperation,
    AssetPatch,
    DataMigration,
    Environment,
    MyState,
    OperationResult,
    state_operations_error,
    state_operations_result,
)
from db import mongo, start_port_forward, stop_port_forward
from llm_utils import call_with_self_heal
from logger import get_logger
from langchain.prompts import PromptTemplate

from operation_helpers import create_patches
from asset_spec import ASSET_SPECS

logger = get_logger(__name__)


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


def make_supplier_library_entry_deprecation_node(llm: ChatOpenAI):
    def supplier_library_entry_deprecation_node(state: MyState) -> MyState:
        data_migration = state.get("task")
        if not isinstance(data_migration, DataMigration):
            logger.error("Task is not a DataMigration")
            return state_operations_error("Task is not a DataMigration")

        data = data_migration.data
        if not data:
            logger.error("No data in DataMigration task")
            return state_operations_error("No data in DataMigration task")

        op_index = state.get("op_index")
        op = state.get("op")
        if op is None:
            return state_operations_error("No op in state")

        messages = [
            SystemMessage(content=DATA_EXTRACTION_PROMPT),
            HumanMessage(
                content=PromptTemplate.from_template(USER_PROMPT_TEMPLATE).format(
                    input_data=json.dumps(data),
                    task_description=state.get("user_input"),
                )
            ),
        ]

        results = call_with_self_heal(messages, llm, Response).results
        xs = [x.model_dump() for x in results]
        logger.info(f"Deprecation results:\n```json\n{json.dumps(xs, indent=2)}\n```")

        sub_operations: List[AssetOperation] = []
        task_description = data_migration.body or ""
        environments = data_migration.environments or []

        enrich = ENRICHERS.get(("SupplierLibraryEntry", "deprecation"))
        if enrich is None:
            logger.error("No enricher found for SupplierLibraryEntry deprecation")
            return state_operations_error("No enricher found for SupplierLibraryEntry deprecation")

        asset_spec = ASSET_SPECS["SupplierLibraryEntry"]
        for deprecation in results:

            # deprecate old entry

            update_op = AssetOperation(
                asset_type="SupplierLibraryEntry", operation_name="update"
            )
            update_op_patches = create_patches(
                llm,
                asset_type="SupplierLibraryEntry",
                operation_name="update",
                asset_spec=asset_spec,
                input_data=[deprecation.update_data],
                task_description=task_description,
            )
            update_op.patches = {env: update_op_patches for env in environments}
            sub_operations.append(update_op)

            # create new library entry

            create_op = AssetOperation(
                asset_type="SupplierLibraryEntry",
                operation_name="create",
            )
            create_op_patches = create_patches(
                llm,
                asset_type="SupplierLibraryEntry",
                operation_name="create",
                asset_spec=asset_spec,
                input_data=[deprecation.create_data],
                task_description=task_description,
            )
            create_op.patches = {env: create_op_patches for env in environments}
            for env, ps in create_op.patches.items():
                for p in ps:
                    try:
                        start_port_forward(env)
                        prev_vat_code = update_op_patches[0].predicate["key"]
                        enrich(p, prev_vat_code)
                    except Exception as e:
                        logger.error(f"Error enriching patch {p}: {e}", exc_info=True)
                    finally:
                        stop_port_forward()
            sub_operations.append(create_op)

            # update organization vatCode and sapCode

            patch = AssetPatch(
                predicate=deprecation.organization_patch["predicate"],
                patch=deprecation.organization_patch["patch"],
            )
            patches: Dict[Environment, List[AssetPatch]] = {
                env: [patch] for env in environments
            }
            org_op = AssetOperation(
                asset_type="Organization",
                operation_name="update",
                patches=patches,
            )
            sub_operations.append(org_op)

        return state_operations_result(
            OperationResult(index=op_index or 0, operations=sub_operations)
        )

    return supplier_library_entry_deprecation_node
