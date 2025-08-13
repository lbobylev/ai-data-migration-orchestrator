from langchain_openai import ChatOpenAI
from app_types import (
    MyState,
)

from logger import get_logger
from operation_helpers import confirm, run_tasks_with_port_forwarding


logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def task_execution_node(state: MyState) -> MyState:
    tasks = state.get("tasks") or {}
    dry_run = state.get("dry_run") or True

    try:
        if len(tasks) == 0:
            logger.info("No tasks to execute.")
            return {"status": "no_task_to_execute"}

        for env, tasks in tasks.items():
            task_descriptions = "\n".join(
                [
                    f"{task.operation} {len(task.patches)} {task.asset_type}"
                    for task in tasks
                ]
            )

            if confirm(
                f"The following tasks will be executed in {env} dry_run {dry_run}:\n{task_descriptions}\nProceed? (y/n): "
            ):
                run_tasks_with_port_forwarding(env, tasks, dry_run=dry_run)
        return {"status": "tasks_executed"}

    except Exception as e:
        logger.error(f"Error executing tasks: {e}", exc_info=True)
        return {"status": "task_execution_failed"}
