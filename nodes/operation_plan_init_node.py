from langchain_openai import ChatOpenAI
from app_types import (
    MyState,
)
from logger import get_logger

logger = get_logger(__name__)


def make_operation_plan_init_node(llm: ChatOpenAI):
    def operation_plan_init_node(state: MyState) -> MyState:
        logger.info("Initializing operation plan...")

        total = len(state.get("detected_operations") or [])
        if total == 0:
            logger.error("No operations detected in state")
            return {}

        return {
            "operation_results": [],
            "operation_errors": [],
            "operation_total": total,
            "operation_done": 0,
        }

    return operation_plan_init_node
