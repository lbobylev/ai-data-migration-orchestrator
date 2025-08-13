from typing import List, Optional

from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from llm_utils import call_with_self_heal

from app_types import (
    AssetType,
    MyState,
)
from logger import get_logger

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, top_p=1)


def _choose_asset_type(detected_asset_types: List[AssetType]) -> AssetType:
    while True:
        choice = input(
            f"Multiple asset types detected:\n{"\n".join([f"{i+1}. {at}" for i, at in enumerate(detected_asset_types)])}\nPlease choose one by entering the corresponding number: "
        )
        if choice.isdigit():
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(detected_asset_types):
                return detected_asset_types[choice_idx]
        print("Invalid choice. Please try again.")


def asset_type_detection_node(state: MyState) -> MyState:
    logger.info("Starting asset type detection node.")

    asset_types = ", ".join(AssetType.__args__)
    messages = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You are a classifier that determines the list of asset types the operation will be performed on based on the user's request.
Return **only** a valid JSON object in the following format (no comments, explanations, or extra text):
{{
    "asset_types": ["<AssetType1>", "<AssetType2>"]
}}
The list of valid asset types is: {asset_types}.
Ensure proper handling of assignments. If the user request specifies a type, it could be one of the following: "Assigned eyewear" or similar may refer to an EyewearManufacturerAssignment.
    """,
            ),
            ("user", "{user_prompt}"),
        ]
    ).format_messages(
        user_prompt=state.get("user_prompt"),
        asset_types=asset_types,
    )

    class Response(BaseModel):
        asset_types: List[AssetType]

    try:
        detected_asset_types = call_with_self_heal(
            llm=llm,
            messages=messages,
            schema_model=Response,
        ).asset_types

        asset_type: Optional[AssetType] = None
        if len(detected_asset_types) > 1:
            asset_type = _choose_asset_type(detected_asset_types)
        elif len(detected_asset_types) == 1:
            asset_type = detected_asset_types[0]

        if asset_type is None:
            logger.error("No asset type detected.")
            return {"status": "asset_type_detection_failed"}

        logger.info(f"Detected asset type: {asset_type}")
        return {"asset_type": asset_type, "status": "asset_type_detected"}

    except Exception as e:
        logger.error(f"Asset type detection failed: {e}")
        return {"status": "asset_type_detection_failed"}
