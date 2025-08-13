from app_types import MyState
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
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def _extract_data_from_text(llm: ChatOpenAI, text: str) -> List[Dict[str, str]]:
    """
    Extracts tabular data from text if present.
    This is a placeholder function and should be implemented with actual logic to parse tables from text.
    """

    logger.info("Extracting tabular data from text...")

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
        llm,
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=text),
        ],
        Response,
    )

    return response.data


def data_extraction_node(state: MyState) -> MyState:
    data_source = state.get("data_source")
    if data_source == "user_request":
        body = state.get("user_input") or ""
        if len(body.strip()) > 0:
            try:
                data = _extract_data_from_text(llm, body)

                if len(data) == 0:
                    logger.info("No tabular data extracted from user request.")
                    return {"status": "no_tabular_data_found"}

                logger.info(f"Extracted {len(data)} records from text")

                return {
                    "status": "data_extracted",
                    "data": data,
                }

            except Exception as e:
                logger.error(f"Error extracting data from text: {e}")
        else:
            logger.error("No body found in user request.")
    else:
        logger.error("Data source is not 'user_request'.")

    return {"status": "data_extraction_failed"}
