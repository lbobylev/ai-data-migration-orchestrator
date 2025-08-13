import json
from typing import List
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END
from langgraph.types import Send
from pydantic import BaseModel

from app_types import DataMigration, MyState, ASSET_OPERATIONS
from asset_spec import ASSET_SPECS
from llm_utils import call_with_self_heal
from logger import get_logger

logger = get_logger()


class DataSplitResponse(BaseModel):
    update: list[dict]
    create: list[dict]
    delete: list[dict]

    def __getitem__(self, item):
        return getattr(self, item)


SYSTEM_PROMPT = f"""
You will be given:
1. A JSON array of input data objects provided by the user.  
2. A list of available operations that can be performed on the data.  
3. A description of the overall task, as well as descriptions of each operation.  

Notes:
- Some data objects may include a field that indicates their purpose (e.g., create, update, delete).  
- Your task is to **split the data into chunks**, where each chunk corresponds to a specific operation.  
- Each operation should contain only the data objects that should be processed by it.  
- If the task description or data object fields mention `"new"`, `"update"`, or `"review"`, map them to the **update operation**.  
- If neither the task description nor data fields point to `"delete"` or `"update"`, assume the default operation is **create**.  
- If no data for the operation is found, return an empty array for that operation.

Output format:
Return a single JSON object structured as follows:
{{
    "update": [ <list of data objects to be processed by update operation> ],
    "create": [ <list of data objects to be processed by create operation> ],
    "delete": [ <list of data objects to be processed by delete operation> ]
}}

Available operations:
{", ".join(f'"{v}": "{k}"' for k, v in ASSET_OPERATIONS.items())}
"""


def make_operation_plan_fanout_node(llm: ChatOpenAI):
    def operation_plan_fanout_node(state: MyState) -> List[Send]:
        ops = state.get("detected_operations") or []
        data_migration = state.get("task")

        if not isinstance(data_migration, DataMigration):
            logger.error("No valid data migration task found in state")
            return []

        if data_migration.data is None or len(data_migration.data) == 0:
            logger.warning("Data migration task has no data")
            return []

        data = data_migration.data or []

        if len(ops) > 1 and len(data) > 1:
            op_names = [op.operation for op in ops]
            user_prompt = f"""
                Task description: {state.get("user_input")}
                Available operations: {", ".join(op_names)}
                Input data: {json.dumps(data, indent=2)}
            """
            messages = [
                SystemMessage(content=SYSTEM_PROMPT),
                HumanMessage(content=user_prompt),
            ]

            logger.info("Calling LLM to split data among operations...")
            response = call_with_self_heal(messages, llm, DataSplitResponse)
            logger.info(
                f"LLM response received and parsed.\n```json\n{response.model_dump_json(indent=2)}\n```"
            )

            sum = 0
            for _, v in response:
                sum += len(v)

            if sum != len(data):
                logger.error(
                    f"Data split inconsistency: expected {len(data)} items, but got {sum} items from LLM."
                )
                return [Send(END, {"status": "data_split_failed"})]

            for op in ops:
                op.data = response[op.operation]
        else:
            ops[0].data = data

        sends = []
        for i, op in enumerate(ops):
            asset_spec = ASSET_SPECS.get(op["asset_type"])

            if asset_spec is not None:
                op["asset_spec"] = asset_spec
            else:
                logger.warning(
                    f"Asset spec not found for asset type: {op['asset_type']}"
                )

            sends.append(
                Send(
                    "operation_worker_node",
                    {"op": op, "op_index": i, "task": data_migration},
                )
            )

        return sends

    return operation_plan_fanout_node
