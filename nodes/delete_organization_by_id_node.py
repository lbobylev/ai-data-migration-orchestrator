from typing import Dict, List
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from app_types import (
    AssetPatch,
    Environment,
    MyState,
)

from logger import get_logger
from operation_helpers import (
    ExecutionTask,
)
from llm_utils import call_with_self_heal
from langchain.prompts import ChatPromptTemplate

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def delete_organization_by_id_node(state: MyState) -> MyState:
    environments = state.get("environments") or []
    user_input = state.get("user_input") or ""

    class Response(BaseModel):
        company_id: str
        dry_run: bool = True

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You are a helpful assistant that extracts the organization ID from the user's request.
You must respond in JSON format with the following fields:
{{
    "company_id": string,  // The ID of the organization to be deleted
    "dry_run": boolean 
}}
**Important**: dry_run can only be false if the user explicitly states they want to proceed with the deletion, otherwise it must be true.
""",
            ),
            ("user", "{user_input}"),
        ]
    )

    try:
        response = call_with_self_heal(
            llm, prompt.format_messages(user_input=user_input), Response
        )
        company_id = response.company_id
        dry_run = response.dry_run

        tasks: Dict[Environment, List[ExecutionTask]] = {
            env: [
                ExecutionTask(
                    asset_type="Organization",
                    operation="delete",
                    patches=[AssetPatch(predicate={"companyId": company_id}, patch={})],
                )
            ]
            for env in environments
        }

        return {"status": "tasks_created", "tasks": tasks, "dry_run": dry_run}

    except Exception as e:
        logger.error(f"Error in delete_organization_by_id_node: {e}")
        return {"status": "task_creation_failed"}
