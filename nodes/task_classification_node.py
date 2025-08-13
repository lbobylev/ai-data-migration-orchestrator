from typing import Dict

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate

from app_types import MyState, TaskType
from llm_utils import retry_call
from logger import get_logger

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1.0)

TASK_DESCRIPTIONS: Dict[TaskType, str] = {
    "data_migration": """
This task involves changing, replacing, remapping, deleting, resetting or updating assets.
It can be a deletion or update request by some criteria.
""",
    "bug": """
This task involves resolving a bug in the system where expected functionality is not working as intended.
The issue may present as a broken feature, a missing feature, or incorrect system behavior that requires correction.
Examples include export or import failures, missing or incomplete data, or features not performing according to requirements.
""",
    "delete_notifications": "This task involves **strictly** deleting notifications and nothing else.",
    "other": "This task does not involve data migration or asset updates.",
}


def task_classification_node(state: MyState) -> MyState:
    template = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You are a classifier. Return EXACTLY one identifier from the allowed list.
Allowed identifiers and their meanings: {task_descriptions}.
"Output must be exactly one of the identifiers {task_identifiers}, with no punctuation or explanation.",
""",
            ),
            ("user", "{user_prompt}"),
        ]
    )

    task_descriptions = "\n".join(
        [
            f"{task_type}: {description}"
            for task_type, description in TASK_DESCRIPTIONS.items()
        ]
    )
    task_entifiers = ", ".join(TASK_DESCRIPTIONS.keys())

    user_prompt = state.get("user_prompt")
    if not user_prompt:
        logger.error("User prompt is empty or not provided.")
        return {**state, "status": "task_classification_failed"}

    try:
        response = retry_call(template | llm).invoke(
            {
                "task_descriptions": task_descriptions,
                "task_identifiers": task_entifiers,
                "user_prompt": user_prompt,
            },
        )
        task_type = str(response.content).strip().lower()

        while True:
            answer = (
                input(f"Detected task type: {task_type}. Is this correct? (y/n): ")
                .strip()
                .lower()
            )
            if answer == "y":
                break
            elif answer == "n":
                task_typs_num_desc = "\n".join(
                    [
                        f"{i+1}. {tt} - {desc}"
                        for i, (tt, desc) in enumerate(TASK_DESCRIPTIONS.items())
                    ]
                )
                answer = input(
                    f"Please enter the correct task type from the list below by number:\n{task_typs_num_desc}\n"
                ).strip()
                task_type = list(TASK_DESCRIPTIONS.keys())[int(answer) - 1]
            else:
                print("Please answer with 'y' or 'n'.")

        logger.info("Task classification result: %s", task_type)

        state_updates: Dict[str, MyState] = {
            "data_migration": {
                "status": "data_migration_detected",
                "task_type": "data_migration",
            },
            "bug": {
                "status": "bug_detected",
                "task_type": "bug",
            },
            "delete_notifications": {
                "status": "delete_notifications_detected",
                "task_type": "delete_notifications",
            },
        }

        new_state = state_updates.get(task_type, None)
        if new_state is None:
            logger.error("Task classification failed, stopping processing.")
            logger.info("********************************************************")
            logger.info(user_prompt)
            logger.info("********************************************************")
            return {"status": "task_classification_failed"}

        return new_state
        
                
    except Exception as e:
        logger.error(f"Error during task classification: {e}")
        return {"status": "task_classification_failed"}
