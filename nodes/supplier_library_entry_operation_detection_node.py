from langchain_openai import ChatOpenAI
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


def supplier_library_entry_operation_detection_node(state: MyState) -> MyState:
    user_prompt = state.get("user_prompt")
    if user_prompt is None:
        logger.error("User input is missing, cannot classify operation.")
        return {"status": "operation_detection_failed"}

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You will receive a user request.  
Your task is to determine the correct operation to perform.  
You must return a valid JSON object in the following format:  

{{
  "operation": <Operation>
}}

### Rules

#### 1. Allowed Operations
- "operation" must always be one of: {operation_types}

#### 2. SupplierLibraryEntry Special Rules  

Deprecation
- Deprecation does NOT mean removal.  
- Instead, the existing entry should be marked as deprecated (e.g., updating its description with "don't use" or similar).  
- The deprecation process in reality involves two actions:
  1. Updating the existing entry (mark as deprecated).  
  2. Creating a new entry with the correct data.  
- However, for this task, you must treat this as a single "deprecation" operation, never as two separate operations (update + create).  

Creation
- If the request is phrased as "update to add" or anything equivalent, the correct operation is "creation", not "update".  
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

    return {
        "status": "operation_detected",
        "detected_operation": detected_operation,
    }
