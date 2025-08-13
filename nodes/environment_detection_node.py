from typing import List, get_args
import json

from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from llm_utils import call_with_self_heal

from app_types import (
    Environment,
    MyState,
    ENVIRONMENTS,
)
from logger import get_logger

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, top_p=1)


def environment_detction_node(state: MyState) -> MyState:
    logger.info("Starting environment detection node.")

    envs = ", ".join(ENVIRONMENTS)
    user_prompt = state.get("user_prompt")
    if not user_prompt:
        logger.error("User prompt is empty or not provided.")
        return {"status": "environment_detection_failed"}

    messages = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You must return **only** a valid JSON object in the following format (no comments, explanations, or extra text):
{{
  "environments": ["<list of environments>"]
}}

Rules:
    - **Environments**
      - Valid values: {envs}.
      - If the request explicitly specifies one or more environments, extract only those.
      - If the request refers to "all environments," return all of them.
      - If no environment is mentioned, default to all environments."
""",
            ),
            ("user", "{user_prompt}"),
        ]
    ).format_messages(user_prompt=user_prompt, envs=envs)

    class Response(BaseModel):
        environments: List[Environment]

    try:
        environments = call_with_self_heal(
            llm=llm,
            messages=messages,
            schema_model=Response,
        ).environments

        detected_envs = ", ".join(environments)

        while True:
            answer = (
                input(f"Detected environments: {detected_envs}. Confirm? (Y/N): ")
                .strip()
                .lower()
            )
            if answer in ("yes", "y"):
                break
            elif answer in ("no", "n"):
                new_envs = input(
                    f"Please enter the correct environments (comma-separated from {envs}): "
                )
                environments = [
                    x.strip()
                    for x in new_envs.split(",")
                    if x.strip() in get_args(Environment)
                ]
                if environments:
                    logger.info(
                        f"User corrected environments to: {', '.join(environments)}"
                    )
                    break
                else:
                    print("No valid environments entered. Please try again.")
            else:
                print("Please enter Yes/No or Y/N.")

        return {"status": "environment_detected", "environments": environments}  # type: ignore

    except Exception as e:
        logger.error(
            f"Environment detection failed:\n```json{json.dumps(e, indent=2)}\n```"
        )
        return {"status": "environment_detection_failed"}
