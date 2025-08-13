import json
from typing import Dict, List
from langchain_openai import ChatOpenAI
from app_types import (
    Environment,
    MyState,
)

from logger import get_logger
from operation_helpers import (
    ExecutionTask,
    confirm,
    create_enriched_patches,
    run_tasks_with_port_forwarding,
)


logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def supplier_library_entry_creation_node(state: MyState) -> MyState:
    environments = state.get("environments") or []
    task_description = state.get("user_input") or ""
    data = state.get("data") or []

    logger.debug(f"Environments: {environments}")
    logger.debug(f"Task Description: {task_description}")
    logger.debug(f"Data: {data}")

    try:
        logger.debug("Starting SupplierLibraryEntry creation process...")
        supplier_library_entry_patches_by_env = create_enriched_patches(
            llm,
            asset_type="SupplierLibraryEntry",
            operation_name="create",
            environments=environments,
            task_description=task_description,
            data=data,
        )
        logger.debug("Creating associated Organization patches...")
        organization_patches_by_env = create_enriched_patches(
            llm,
            asset_type="Organization",
            operation_name="create",
            environments=environments,
            task_description=task_description,
            data=data,
        )
        logger.debug("Combining patches into execution tasks...")
        tasks: Dict[Environment, List[ExecutionTask]] = {}
        for env, patches in organization_patches_by_env.items():
            if env not in tasks:
                tasks[env] = []
            tasks[env].append(
                ExecutionTask(
                    asset_type="Organization",
                    operation="create",
                    patches=patches,
                )
            )
        for env, patches in supplier_library_entry_patches_by_env.items():
            tasks[env].append(
                ExecutionTask(
                    asset_type="SupplierLibraryEntry",
                    operation="create",
                    patches=patches,
                )
            )
        logger.debug("Prepared tasks for all environments.")
        for env in environments:
            env_tasks = tasks.get(env) or []

            tasks_json = json.dumps([t.model_dump() for t in env_tasks], indent=2)

            logger.debug(
                    f"Tasks:\n```json\n{tasks_json}\n```"
            )

            logger.debug(
                f"Prepared {len(env_tasks)} tasks for environment {env}"
            )

            if confirm(
                f"Execute creation of {len(env_tasks)} assets in {env}? (y/n): "
            ):
                run_tasks_with_port_forwarding(env, env_tasks, dry_run=False)

        return {"status": "operation_processed"}

    except Exception as e:
        logger.error(
            f"Error processing SupplierLibraryEntry creation: {e}", exc_info=True
        )
        return {"status": "operation_processing_failed"}
