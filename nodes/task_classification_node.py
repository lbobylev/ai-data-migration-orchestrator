from typing import Dict

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app_types import Bug, DataMigration, MyState, TaskType
from logger import get_logger
from llm_utils import retry_call

logger = get_logger()

TASK_DESCRIPTIONS: Dict[TaskType, str] = {
    "data_migration": "This task involves changing, replacing, remapping, or updating assets.",
    "bug": """
This task involves resolving a bug in the system where expected functionality is not working as intended.
The issue may present as a broken feature, a missing feature, or incorrect system behavior that requires correction.
Examples include export or import failures, missing or incomplete data, or features not performing according to requirements.
""",
    "other": "This task does not involve data migration or asset updates.",
}


def make_task_classification_node(llm: ChatOpenAI):
    def task_classification_node(state: MyState) -> MyState:
        lines = [
            "You are a classifier. Return EXACTLY one identifier from the allowed list.",
            "Allowed identifiers and their meanings:",
        ]
        for task_type, description in TASK_DESCRIPTIONS.items():
            lines.append(f"{task_type} - {description}")
        lines.append(
            f"Output must be exactly one of the identifiers {', '.join(TASK_DESCRIPTIONS.keys())}, with no punctuation or explanation."
        )
        system_prompt = "\n".join(lines)

        user_prompt = state.get("user_prompt")
        if not user_prompt:
            logger.error("User prompt is empty or not provided.")
            return {**state, "status": "task_classification_failed"}

        response = retry_call(
            lambda: llm.invoke(
                [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=user_prompt),
                ]
            )
        )

        task_type = str(response.content).strip().lower()
        match task_type:
            case "data_migration":
                logger.info("Data migration detected, proceeding to classification.")
                return {**state, "status": "data_migration_detected", "task": DataMigration()}
            case "bug":
                logger.info("Bug detected, proceeding to classification.")
                return {**state, "status": "bug_detected", "task": Bug()}
            case _:
                logger.error("Task classification failed, stopping processing.")
                logger.info("********************************************************")
                logger.info(user_prompt)
                logger.info("********************************************************")
                return {**state, "status": "task_classification_failed"}

    return task_classification_node
