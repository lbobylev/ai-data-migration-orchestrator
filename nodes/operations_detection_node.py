from typing import Dict, List
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, model_validator

from app_types import (
    AssetOperation,
    AssetType,
    DataMigration,
    MyState,
    ASSET_OPERATIONS,
    ASSET_TYPES,
)
from logger import get_logger
from llm_utils import call_with_self_heal
from langchain.prompts import PromptTemplate

logger = get_logger()


SYSTEM_PROMPT = """
You will receive a user request and the asset type detected from the request.
Your task is for the current asset type, determine the operation to be performed on it.
You must return a valid JSON object in the following format:
{{
    "operations": [
        {{
            "asset_type": "<AssetType>",
            "operation_name": "<Operation>",
        }}
    ]
}}

Rules:
- AssetType
  - Must be one of: {asset_types}.

- Operation
  - Must be one of: {operation_types}.
"""

PROMPTS_BY_ASSET_TYPE: Dict[AssetType, str] = {
    "SupplierLibraryEntry": """
## Prompt for Handling `SupplierLibraryEntry` Operations

When processing operations related to the `SupplierLibraryEntry` asset type, follow these rules:

### 1. Deprecation
- Deprecation does **not** mean removal. Instead, the existing entry should be marked as deprecated (e.g., updating the description field to include "don't use" or similar text).
    - In such cases, the process involves:
        1. Updating the existing entry (to mark it deprecated).
        2. Creating a new entry with the correct data.
    **Important**: it is critical to treat this as **one single deprecation operation**, rather than two separate operations (update + create).
- Even though the real-world process involves both updating the old entry and creating a new one, you must **never represent these as separate operations**.  
- Instead, always output **only one operation** with:
  - `"operation_name": "deprecation"`
  - `"asset_type": "SupplierLibraryEntry"`
- **Important**: If deprecation is detected no other operations should be included in the output.

✅ Correct:
{
  "operations": [
    {
      "operation_name": "deprecation",
      "asset_type": "SupplierLibraryEntry"
    }
  ]
}

❌ Incorrect (**do not do this**):
{
  "operations": [
    {
      "operation_name": "deprecation",
      "asset_type": "SupplierLibraryEntry"
    },
    {
      "operation_name": "create",
      "asset_type": "SupplierLibraryEntry"
    },
    {
      "operation_name": "create",
      "asset_type": "Organization"
    }
  ]
}


---

### 2. Creation
- When creating a new `SupplierLibraryEntry`, you must also create an associated `Organization`.  
- This means that **creation always produces exactly two operations**:  

{
  "operations": [
    {
      "operation_name": "create",
      "asset_type": "SupplierLibraryEntry"
    },
    {
      "operation_name": "create",
      "asset_type": "Organization"
    }
  ]
}

---

### 3. General Rules
- Do **not** generate an operation per record in the input data.  
- Always output **one operation per asset type**.  
- **Deprecation = exactly one operation.**  
- **Creation = exactly two operations (`SupplierLibraryEntry` + `Organization`).**  
- No other combinations are allowed.  
"""
}


def make_operation_detection_node(llm: ChatOpenAI):
    def operation_detection_node(state: MyState) -> MyState:
        user_prompt = state.get("user_prompt")
        if user_prompt is None:
            logger.error("User input is missing, cannot classify data migration.")
            return {**state, "status": "data_migration_classification_failed"}

        asset_types = state.get("detected_asset_types") or []
        if len(asset_types) == 0:
            logger.error("No asset types detected, cannot classify operations.")
            return {**state, "status": "operations_detection_failed"}

        for asset_type in asset_types:
            system_prompt = PromptTemplate(
                template=SYSTEM_PROMPT,
                input_variables=["asset_types", "operation_types"],
            ).format(
                asset_types=", ".join(ASSET_TYPES),
                operation_types=", ".join(ASSET_OPERATIONS.keys()),
            )

            messages: List[BaseMessage] = [SystemMessage(content=system_prompt)]
            if asset_type in PROMPTS_BY_ASSET_TYPE:
                prompt_by_asset = PROMPTS_BY_ASSET_TYPE[asset_type]
                messages.append(SystemMessage(content=prompt_by_asset))
            messages.append(HumanMessage(content=user_prompt))

            # dumped_messages: str = "\n".join(
            #     [f"```log\n{type(m).__name__}: {m.content}```" for m in messages]
            # )
            #
            # logger.info(f"Operation detection prompt:\n{dumped_messages}")

            class Response(BaseModel):
                operations: List[AssetOperation]

                @model_validator(mode="after")
                def validate_operations(self):
                    ops = self.operations
                    deprecation = next(
                        (op for op in ops if op.operation_name == "deprecation"),
                        None,
                    )
                    if deprecation is not None and len(ops) > 1:
                        raise ValueError(
                            "If deprecation is present, it must be the only operation."
                        )
                    return self

            response = call_with_self_heal(messages, llm, Response)
            operations = response.operations

            if len(operations) == 0:
                logger.error("No operations detected, stopping processing.")
                return {**state, "status": "operations_detection_failed"}

        logger.info(
            f"Detected operations:\n```json\n{response.model_dump_json(indent=2)}\n```"
        )

        data_migration = state.get("task")
        if not isinstance(data_migration, DataMigration):
            logger.error("No valid data migration task found in state")
            return {**state, "status": "operations_detection_failed"}

        prev_task_data = data_migration.model_dump()
        prev_task_data.pop("operations", None)
        # logger.info(
        #     f"New DataMigration task:\n```json\n{new_op.model_dump_json(indent=2)}\n```"
        # )

        return {
            "status": "operations_detected",
            "detected_operations": operations,
            "task": DataMigration(**prev_task_data, operations=operations),
        }

    return operation_detection_node
