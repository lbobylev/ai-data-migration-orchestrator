import json
from langchain_openai import ChatOpenAI
from app_types import (
    AssetType,
    DataMigration,
    MyState,
    Operation,
    OperationResult,
    state_operations_error,
    state_operations_result,
    AssetPatch,
)
from typing import Any, Callable, Dict, Tuple

from asset_spec import ASSET_SPECS
from logger import get_logger
from operation_helpers import create_patches
from enrichers import ENRICHERS
from db import start_port_forward, stop_port_forward

logger = get_logger(__name__)

OperationHandler = Callable[[DataMigration, int], Any]
OPERATION_HANDLERS: Dict[Tuple[AssetType, Operation], OperationHandler] = {}


def register_handler(asset_type: AssetType, operation: Operation) -> Callable:
    def deco(fn: OperationHandler) -> OperationHandler:
        OPERATION_HANDLERS[(asset_type, operation)] = fn
        return fn

    return deco


def make_operation_worker_node(llm: ChatOpenAI):
    def operation_worker_node(state: MyState) -> MyState:
        data_migration = state.get("task")
        if not data_migration or not isinstance(data_migration, DataMigration):
            return state_operations_error("No DataMigration task in state")

        op = state.get("op")
        if not op:
            logger.warning("No operation detected in state")
            return state_operations_error("No op in state")

        key = (op.asset_type, op.operation_name)
        op_index = state.get("op_index")
        if op_index is None:
            logger.warning("No op_index in state")
            return state_operations_error("No op_index in state", op)

        handler = OPERATION_HANDLERS.get(key)
        payload = {}

        try:
            if handler is None:
                asset_spec = ASSET_SPECS.get(op.asset_type)
                if asset_spec is None:
                    logger.error(f"No asset spec found for asset type: {op.asset_type}")
                    return state_operations_error(
                        f"No asset spec found for asset type: {op.asset_type}", op
                    )

                environments = data_migration.environments
                task_description = data_migration.body or ""
                patches = create_patches(
                    llm,
                    asset_type=op.asset_type,
                    operation_name=op.operation_name,
                    asset_spec=asset_spec,
                    input_data=op.data,
                    task_description=task_description,
                )
                op.patches = {
                    env: [AssetPatch(**p.model_dump()) for p in patches]
                    for env in environments
                }
                patches_json = {
                    k: [m.model_dump() for m in v] for k, v in op.patches.items()
                }
                logger.info(
                    f"Mapped data:\n```json\n{json.dumps(patches_json, indent=2)}\n```"
                )

                enrich = ENRICHERS.get((op.asset_type, op.operation_name), None)
                if enrich is not None:
                    logger.info(
                        f"Enriching operation {op.asset_type} {op.operation_name}"
                    )
                    for env, ps in op.patches.items():
                        logger.info(f"Enriching patches for environment {env}...")
                        try:
                            start_port_forward(env)
                            for i, p in enumerate(ps):
                                enrich(p)
                                logger.debug(f"Enriched patch {i}")
                        except Exception as e:
                            logger.error(
                                f"Error enriching patch {i}\n```json\n{i}\n```\n\n {e}",
                                exc_info=True,
                            )
                            return state_operations_error(
                                f"Error enriching patch: {e}", op
                            )
                        finally:
                            stop_port_forward()
                logger.info(f"Operation {op.asset_type} {op.operation_name} ready.")

            else:
                payload = handler(data_migration, op_index)

            result = OperationResult(index=op_index, operations=[op], **payload)

            return state_operations_result(result)

        except Exception as e:
            logger.error(f"Error processing operation {op}: {e}", exc_info=True)
            return state_operations_error(str(e), op)

    return operation_worker_node
