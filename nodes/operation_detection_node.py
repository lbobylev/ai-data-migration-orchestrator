from langchain_openai import ChatOpenAI
from langgraph.graph import END
from langgraph.types import Send
from pydantic import BaseModel

from app_types import (
    MyState,
    ASSET_OPERATIONS,
    Operation,
)
from logger import get_logger
from llm_utils import call_with_self_heal
from langchain.prompts import ChatPromptTemplate

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def operation_detection_node(state: MyState) -> Send:
    user_prompt = state.get("user_prompt")
    if user_prompt is None:
        logger.error("User input is missing, cannot classify operation.")
        return Send(END, {"status": "operation_detection_failed"})

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You will receive a user request.
Your task is to determine the operation to be performed.
You must return a valid JSON object in the following format:
{{
    "operation": <Operation>
}}

Rules:
- Operation
  - Must be one of: {operation_types}.
""",
            ),
            ("user", user_prompt),
        ]
    )

    messages = prompt.format_messages(
        operation_types=", ".join(ASSET_OPERATIONS.keys()),
        user_prompt=user_prompt,
    )

    class Response(BaseModel):
        operation: Operation

    detected_operation = call_with_self_heal(llm, messages, Response).operation

    while True:
        answer = (
            input(f"Detected operation: {detected_operation}. Is this correct? (y/n): ")
            .strip()
            .lower()
        )
        if answer == "y":
            break
        elif answer == "n":
            ops = "\n".join(
                [
                    f"{i+1}. {op} - {ASSET_OPERATIONS[op]}"
                    for i, op in enumerate(ASSET_OPERATIONS.keys())
                ]
            )

            answer = input(
                f"Please enter the correct operation from the list below by number:\n{ops}\n"
            ).strip()
            detected_operation = list(ASSET_OPERATIONS.keys())[int(answer) - 1]
            break

    status = state.get("status")
    next_node: str
    match status:
        case "data_extracted":
            next_node = "task_creation_node"
        case _:
            next_node = "patch_extraction_node"

    return Send(
        next_node,
        {
            **state,
            "status": "operation_detected",
            "detected_operation": detected_operation,
        },
    )
