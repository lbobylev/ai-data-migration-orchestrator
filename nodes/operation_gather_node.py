import json
from app_types import MyState
from logger import get_logger

logger = get_logger()


def operation_gather_node(state: MyState):
    done = state.get("done") or 0
    total = state.get("total") or 0
    if done >= total:
        logger.info(f"All operations done: {done}/{total}")
        errors = state.get("errors") or []
        logger.info(f"Errors:\n```log\n{errors}\n```")
        return state
