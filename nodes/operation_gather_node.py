import json
from typing import List, Sequence, TypeVar
from app_types import (
    DataMigration,
    MyState,
)
from logger import get_logger
from operation_helpers import ExecutionTask
from bc.run_tasks import run_tasks
from bc.kube_utils import PortForwardHandle, start_port_forwarding, stop_port_forwarding

logger = get_logger(__name__)

T = TypeVar("T")

def flatten(seq: Sequence[List[T] | T]) -> List[T]:
    result = []
    for item in seq:
        if isinstance(item, list):
            result.extend(flatten(item))  # recursive call
        else:
            result.append(item)
    return result


def operation_gather_node(state: MyState):
    logger.info("Operation gather node started.")
    done = state.get("operation_done") or 0
    total = state.get("operation_total") or 0
    if done >= total:
        logger.info(f"All operations done: {done}/{total}")
        errors = state.get("operation_errors") or []
        error_msgs = [e["error"] for e in errors if "error" in e]
        if len(errors) > 0:
            logger.info(f"Errors:\n```log\n{error_msgs}\n```")
        else:
            results = state.get("operation_results") or []
            ops = flatten([result["operations"] for result in results])
            logger.info(f"Total operations gathered: {len(ops)}")
            # logger.info(
            #     f"Operations:\n```json\n{json.dumps([op.model_dump() for op in ops], indent=2)}\n```"
            # )
            data_migration = state.get("task")
            if not isinstance(data_migration, DataMigration):
                logger.error("No valid DataMigration task in state")
                return state
            if len(ops) == 0:
                logger.warning("No operations to execute.")
            envs = data_migration.environments or []
            if len(envs) == 0:
                logger.warning("No environments specified.")
            for env in envs:
                tasks = []
                for op in ops:
                    if env not in op.patches:
                        logger.warning(
                            f"No patches for operation {op.asset_type} {op.operation_name} in environment {env}, skipping."
                        )
                        continue
                    patches = op.patches[env]
                    if len(patches) == 0:
                        logger.warning(
                            f"No patches for operation {op.asset_type} {op.operation_name} in environment {env}, skipping."
                        )
                        continue
                    tasks.append(
                        ExecutionTask(
                            asset_type=op.asset_type,
                            operation=op.operation_name,
                            patches=patches,
                        )
                    )
                if len(tasks) == 0:
                    logger.warning(
                        f"No tasks to execute in environment {env}, skipping."
                    )
                    continue

                handle: PortForwardHandle
                try:
                    logger.info(f"Executing {len(tasks)} tasks in environment {env}...")
                    logger.info(
                        f"Tasks:\n```json\n{json.dumps([t.model_dump() for t in tasks], indent=2)}\n```"
                    )
                    handle = start_port_forwarding(env)
                    run_tasks(
                        environment=env,
                        tasks=tasks,
                        dry_run=True
                    )
                except Exception as e:
                    logger.error(f"Error executing tasks in environment {env}: {e}")
                finally:
                    if handle:
                        stop_port_forwarding(handle)

        return state
