from typing import get_args

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app_types import (
    DataMigration,
    DataSource,
    MyState,
    ASSET_OPERATIONS,
    ASSET_TYPES,
    ENVIRONMENTS,
)
from confirm import require_confirm
from logger import get_logger
from llm_utils import call_with_self_heal

logger = get_logger()


def make_data_migration_classification_node(llm: ChatOpenAI):
    @require_confirm()
    def data_migration_classification_node(state: MyState) -> MyState:
        system_prompt = f""" 
You are a classifier that determines the type of operation on an asset based on the user's request.

Return a valid JSON object in the following format (no comments, explanations, or extra text):
{{
  "operations": [
    {{
      "asset_type": "<AssetType>",
      "operation": "<Operation>",
    }}
  ],
  "environments": ["<list of environments>"]
  "data_source": "<DataSource>",
  "file_url": "<string|null>"
}}

Rules:
- Environments: Valid values are {", ".join(ENVIRONMENTS)}.
  - If the request refers to all environments, return all of them.
- AssetType: Must be one of {", ".join(ASSET_TYPES)}.
- Operation: Must be one of {", ".join(ASSET_OPERATIONS.keys())}.
- DataSource: Must be one of {", ".join(list(get_args(DataSource)))}.

Data source selection:
1. Use "user_request" if the request explicitly contains asset-related data (tables, lists, objects, fields, IDs, names, or attributes) that can directly identify, parameterize, or manipulate assets.
2. Use "other" if:
   - No file, table, list, object, or identifiable data is provided, OR
   - The request only references assets in general terms.
3. Use "attachment_file" if the request contains an Excel file URL.
   - In this case, set "file_url" to the extracted file URL.
   - Otherwise, "file_url" must be null.

Data usage:
- Asset-related data can be used to:
  - Identify objects for update or deletion
  - Provide object data for creation
  - Match existing assets for retrieval
"""

        user_prompt = state.get("user_prompt")
        if user_prompt is None:
            logger.error("User input is missing, cannot classify data migration.")
            return {**state, "status": "data_migration_classification_failed"}

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]

        data_migration = call_with_self_heal(messages, llm, DataMigration)
        operations = data_migration.operations

        if len(operations) == 0:
            logger.error("No operations detected, stopping processing.")
            return {**state, "status": "data_migration_classification_failed"}

        if data_migration.data_source == "user_request":
            issue = state.get("issue")
            if issue is not None:
                data_migration.body = issue["body"]

        logger.info(
            f"Detected operations:\n```json\n{data_migration.model_dump_json(indent=2)}\n```"
        )

        return {
            **state,
            "status": "data_migration_classified",
            "detected_operations": operations,
            "task": data_migration,
        }

    return data_migration_classification_node
