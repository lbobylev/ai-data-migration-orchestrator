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

def patch_extraction_node(state: MyState) -> MyState:
    logger.info("Starting patch extraction node.")

    data_source = state.get("data_source")
    if data_source == "user_request":
        body = state.get("user_input") or ""
        if len(body.strip()) > 0:
            try:
                # data = _extract_data_from_text(llm, body)
                #
                # if len(data) == 0:
                #     logger.info("No tabular data extracted from user request.")
                #     return {"status": "no_tabular_data_found"}

                patches = []

                if len(patches) > 0:
                    return {
                        "status": "patches_extracted",
                        "patches": patches,
                    }

            except Exception as e:
                logger.error(f"Error extracting patches from text: {e}")
        else:
            logger.error("No body found in user request.")
    else:
        logger.error("Data source is not 'user_request'.")

    return {"status": "patch_extraction_failed"}
