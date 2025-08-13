from typing import List, get_args

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from app_types import (
    AssetType,
    DataMigration,
    DataSource,
    Environment,
    MyState,
    ASSET_TYPES,
    ENVIRONMENTS,
)
from logger import get_logger
from llm_utils import call_with_self_heal

logger = get_logger()


def make_data_migration_classification_node(llm: ChatOpenAI):
    def data_migration_classification_node(state: MyState) -> MyState:
        system_prompt = f""" 
You are a classifier that determines the list of asset types the operation will be performed on based on the user's request. 

Return **only** a valid JSON object in the following format (no comments, explanations, or extra text):

{{
  "asset_types": [
    "<AssetType1>",
    "<AssetType2>"
  ],
  "environments": ["<list of environments>"],
  "data_source": "<DataSource>",
  "file_url": "<string|null>"
}}

Rules:
- **Environments**
  - Valid values: {", ".join(ENVIRONMENTS)}.
  - If the request explicitly specifies one or more environments, extract only those.
  - If the request refers to "all environments," return all of them.
  - If no environment is mentioned, default to all environments.

- **AssetType**
  - Must be one of: {", ".join(ASSET_TYPES)}.

- **DataSource**
  - Must be one of: {", ".join(list(get_args(DataSource)))}.
  - Selection rules:
    1. Use `"user_request"` if the request explicitly contains asset-related data (tables, lists, objects, fields, IDs, names, attributes).
    2. Use `"attachment_file"` if the request contains an Excel file URL.
       - In this case, `"file_url"` must be the extracted URL.
    3. Use `"other"` if:
       - No file, table, list, object, or identifiable data is provided, OR
       - The request only references assets in general terms.
    4. If `"attachment_file"` is not used, `"file_url"` must be `null`.
"""

        user_prompt = state.get("user_prompt")
        if user_prompt is None:
            logger.error("User input is missing, cannot classify data migration.")
            return {**state, "status": "data_migration_classification_failed"}

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]

        class Response(BaseModel):
            asset_types: List[AssetType]
            environments: List[Environment]
            data_source: DataSource
            file_url: str | None

        response = call_with_self_heal(messages, llm, Response)

        if len(response.asset_types) == 0:
            logger.error("No operations detected, stopping processing.")
            return {**state, "status": "data_migration_classification_failed"}

        data_migration = DataMigration(
            environments=response.environments,
            data_source=response.data_source,
            file_url=response.file_url,
        )

        if response.data_source == "user_request":
            issue = state.get("issue")
            if issue is not None:
                data_migration.body = issue["body"]

        logger.info(
            f"Detected operations:\n```json\n{response.model_dump_json(indent=2)}\n```"
        )

        return {
            **state,
            "status": "data_migration_classified",
            "detected_asset_types": response.asset_types,
            "task": data_migration,
        }

    return data_migration_classification_node
