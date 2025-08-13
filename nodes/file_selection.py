from app_types import MyState
from logger import get_logger
from file_utils import select_file

logger = get_logger()

def file_selection_node(state: MyState) -> MyState:
    file_path = select_file()
    if not file_path:
        logger.error("File selection failed, stopping processing.")
        return {**state, "status": "file_selection_failed"}
    task_data = state.get("task_data") or {}
    return {
        **state,
        "status": "file_selected",
    }
