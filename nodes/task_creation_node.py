from typing import Dict, List
from langchain_openai import ChatOpenAI
from app_types import (
    Environment,
    MyState,
)

from logger import get_logger
from operation_helpers import (
    ExecutionTask,
    create_enriched_patches,
)


logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def task_creation_node(state: MyState) -> MyState:
    operation_name = state.get("detected_operation")
    if operation_name is None:
        logger.error("No operation detected, cannot process operation.")
        return {"status": "task_creation_failed"}

    asset_type = state.get("asset_type")
    if asset_type is None:
        logger.error("No asset type detected, cannot process operation.")
        return {"status": "task_creation_failed"}

    environments = state.get("environments") or []
    task_description = state.get("user_input") or ""
    data = state.get("data") or []

    try:
        patches_by_env = create_enriched_patches(
            llm,
            asset_type=asset_type,
            operation_name=operation_name,
            environments=environments,
            task_description=task_description,
            data=data,
        )
        tasks: Dict[Environment, List[ExecutionTask]] = {
            env: [ExecutionTask(
                asset_type=asset_type,
                operation=operation_name,
                patches=patches,
            )]
            for env, patches in patches_by_env.items()
        }

        return {"status": "tasks_created", "tasks": tasks}

    except Exception:
        return {"status": "task_creation_failed"}
