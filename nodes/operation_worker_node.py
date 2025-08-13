import json
from langchain_openai import ChatOpenAI
from app_types import (
    AssetType,
    DataMigration,
    MyState,
    Operation,
)
from typing import Any, Callable, Dict, Tuple

from confirm import require_confirm
from logger import get_logger
from operation_helpers import map_data

logger = get_logger()

OperationHandler = Callable[[DataMigration, int], Any]
OPERATION_HANDLERS: Dict[Tuple[AssetType, Operation], OperationHandler] = {}


def register_handler(asset_type: AssetType, operation: Operation) -> Callable:
    def deco(fn: OperationHandler) -> OperationHandler:
        OPERATION_HANDLERS[(asset_type, operation)] = fn
        return fn

    return deco


def make_operation_worker_node(llm: ChatOpenAI):
    @require_confirm()
    def operation_worker_node(state: MyState) -> MyState:
        data_migration = state.get("task")
        if not data_migration or not isinstance(data_migration, DataMigration):
            return {"errors": [{"error": "No data migration in state"}], "done": 1}

        op = state.get("op")
        if not op:
            return {"errors": [{"error": "No op in state"}], "done": 1}

        key = (op["asset_type"], op["operation"])
        op_index = state.get("op_index")
        if op_index is None:
            return {"errors": [{"error": "No op_index in state"}], "done": 1}
        handler = OPERATION_HANDLERS.get(key)
        payload = {}

        try:
            if handler is None:
                mapped_data = map_data(llm, data_migration, op_index)
                mapped_data_json = json.dumps(
                    [m.model_dump() for m in mapped_data], indent=2
                )
                logger.info(f"Mapped data:\n```json\n{mapped_data_json}\n```")
            else:
                payload = handler(data_migration, op_index)

            res = {
                "index": op_index,
                "asset_type": op.asset_type,
                "operation": op.operation,
                "op": op,
                **payload,
            }

            return {"results": [res], "done": 1}

        except Exception as e:
            return {
                "errors": [{"op": op, "error": str(e)}],
                "done": 1,
            }

    return operation_worker_node
