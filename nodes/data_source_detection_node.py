from typing import get_args

from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from app_types import (
    DataSource,
    MyState,
)
from logger import get_logger
from llm_utils import call_with_self_heal

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def data_source_detection_node(state: MyState) -> MyState:
    data_sources = ", ".join(list(get_args(DataSource)))

    user_prompt = state.get("user_prompt")
    if user_prompt is None:
        logger.error("User input is missing, cannot detect data source.")
        return {"status": "data_source_detection_failed"}

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """ 
Analyze the user's request and determine the appropriate data source and file URL if applicable.
Return **only** a valid JSON object in the following format (no comments, explanations, or extra text):
{{
    "data_source": "<DataSource>",
    "file_url": "<string|null>"
}}

Rules:
- **DataSource**
- Must be one of: {data_sources}.
- Selection rules:
1. Use `"user_request"` if the request explicitly contains asset-related data (tables, lists, objects, fields, IDs, names, attributes).
2. Use `"attachment_file"` if the request contains an Excel file URL.
   - In this case, `"file_url"` must be the extracted URL.
3. Use `"other"` if:
   - No file, table, list, object, or identifiable data is provided, OR
   - The request only references assets in general terms.
4. If `"attachment_file"` is not used, `"file_url"` must be `null`.
""",
            ),
            ("user", "{user_prompt}"),
        ]
    )

    messages = prompt.format_messages( 
        data_sources=data_sources,
        user_prompt=user_prompt,
    )

    class Response(BaseModel):
        data_source: DataSource
        file_url: str | None

    try:
        response = call_with_self_heal(llm, messages, Response)
        detected_data_source = response.data_source

        while True:
            if detected_data_source == "attachment_file":
                answer = (
                    input(
                        f"Detected data source: {detected_data_source} with file URL: {response.file_url}. Confirm? (Y/N): "
                    )
                    .strip()
                    .lower()
                )
            else:
                answer = (
                    input(f"Detected data source: {detected_data_source}. Confirm? (Y/N): ")
                    .strip()
                    .lower()
                )
            if answer in ("yes", "y"):
                return {
                    "status": "data_source_detected",
                    "data_source": detected_data_source,
                    "file_url": response.file_url,
                }
            elif answer in ("no", "n"):
                logger.error(
                    "Data source detection not confirmed by user, stopping processing."
                )
                return {"status": "data_source_detection_failed"}
    except Exception as e:
        logger.error(f"Data source detection failed: {e}")
        return {"status": "data_source_detection_failed"}
