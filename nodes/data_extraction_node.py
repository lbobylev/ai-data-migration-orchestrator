from app_types import DataMigration, MyState
from logger import get_logger

from typing import Dict, List

from langchain_core.messages import (
    HumanMessage,
    SystemMessage,
)
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from llm_utils import call_with_self_heal


logger = get_logger(__name__)


def _extract_data_from_text(llm: ChatOpenAI, text: str) -> List[Dict[str, str]]:
    """
    Extracts tabular data from text if present.
    This is a placeholder function and should be implemented with actual logic to parse tables from text.
    """
    system_prompt = """
You will be given a text that may contain tabular data.
If tabular data is found, extract it and return as a JSON object in the following format:
{
    "data": [
        {<column_name>: <value>, ...},
        ...
    ]
}
If no tabular data is found, the data array must be empty. 
"""

    class Response(BaseModel):
        data: List[Dict[str, str]]

    response = call_with_self_heal(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=text),
        ],
        llm,
        Response,
    )

    if len(response.data) == 0:
        logger.info("No tabular data found in text")
        return []

    logger.info(
        f"Extracted {len(response.data)} records from text:\n```json\n{response.model_dump_json(indent=2)}\n```"
    )

    return response.data


def make_data_extraction_node(llm: ChatOpenAI):
    def data_extraction_node(state: MyState) -> MyState:
        data_migration = state.get("task") or {}
        if isinstance(data_migration, DataMigration):
            if data_migration.data_source == "user_request":
                body = data_migration.body or ""
                if len(body.strip()) > 0:
                    try:
                        data = _extract_data_from_text(llm, body)
                        prev_task = data_migration.model_dump()
                        prev_task.pop("data", None)

                        return {
                            "status": "data_extracted",
                            "task": DataMigration(**prev_task, data=data),
                        }

                    except Exception as e:
                        logger.error(f"Error extracting data from text: {e}")
                else:
                    logger.error("No body found in user request.")
            else:
                logger.error("Data source is not 'user_request'.")
        else:
            logger.error("No valid DataMigration task found in state.")

        return {"status": "data_extraction_failed"}

    return data_extraction_node
