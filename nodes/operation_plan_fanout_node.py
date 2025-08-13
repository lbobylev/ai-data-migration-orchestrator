from typing import Dict, List, Tuple
from langchain_openai import ChatOpenAI
from langgraph.types import Send

from app_types import AssetType, MyState, Operation, DataMigration
from logger import get_logger

logger = get_logger(__name__)

OPERATION_TO_NODE: Dict[Tuple[AssetType, Operation], str] = {
    ("SupplierLibraryEntry", "deprecation"): "supplier_library_entry_derprecation_node",
}


def make_operation_plan_fanout_node(llm: ChatOpenAI):
    def operation_plan_fanout_node(state: MyState) -> List[Send]:
        logger.info("Creating operation plan fanout...")

        ops = state.get("detected_operations") or []

        data_migration = state.get("task")

        sends = []
        for i, op in enumerate(ops):
            

            target_node = OPERATION_TO_NODE.get(
                (op.asset_type, op.operation_name),
                "operation_worker_node",
            )

            if isinstance(data_migration, DataMigration):
                op.data = data_migration.data or []

            logger.info(f"Routing operation {i} to node {target_node}")
            sends.append(
                Send(
                    target_node,
                    {"op": op, "op_index": i, "task": data_migration},
                )
            )

        logger.info(f"Total operations to fanout: {len(sends)}")

        return sends

    return operation_plan_fanout_node
