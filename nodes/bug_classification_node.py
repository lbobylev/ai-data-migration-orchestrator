from typing import get_args
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app_types import Environment, MyState
from app_types import Bug
from llm_utils import call_with_self_heal
from logger import get_logger

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


BUG_DESCRIPTIONS = {
    "export_issue": "This bug involves issues with exporting data or files.",
    "other": "This bug does not fall into the export issue category.",
}

ENVIRONMENTS = list(get_args(Environment))

def bug_classification_node(state: MyState) -> MyState:
    system_prompt = f"""
You are a **bug classifier**. Your task is to classify a bug based on the provided description.  

### Allowed Bug Types  
The valid bug types and their meanings are:  
{', '.join([f"{bug_type} – {description}" for bug_type, description in BUG_DESCRIPTIONS.items()])}  

### Expected Output  
Return a **JSON object** in the following format:  
{{
"bug_type": "<bug_type>",
"environment": ["<environment1>", "<environment2>"],
"orgs": ["<org1>", "<org2>"]
}}

- **bug_type** → must be one of: {', '.join(BUG_DESCRIPTIONS.keys())}  
- **environment** → must be chosen from: {', '.join(ENVIRONMENTS)}  
- **orgs** → must be a list of detected organizations related to the bug  

### Environment Detection Rules  
1. If the description contains a URL, extract the environment from the domain structure:  
- https://omas.cp-bc.com/ → "prod"  
- https://omas.preprod.cp-bc.com/ → "preprod"  
- https://omas.test.cp-bc.com/ → "test"  
- https://kering.dev.cp-bc.com/ → "dev"  
2. If the description explicitly mentions environments (e.g., production, pre-production, test, development), include them.  
3. If multiple environments are mentioned, return **all of them**.  
4. If all environments are detected, return all of them in the list.

### Organization Detection Rules  
1. From URLs, the **organization** is the first domain segment:  
- Example: https://omas.cp-bc.com/ → "omas"  
2. If organizations are mentioned explicitly in the description, extract them as well.  
3. The organizations should be returned as a list of lowercase strings.
"""

    user_prompt = state.get("user_prompt")
    if not user_prompt:
        logger.error("User prompt is empty or not provided.")
        return {**state, "status": "bug_classification_failed"}

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]

    bug = call_with_self_heal(llm, messages, Bug)

    match bug.bug_type:
        case "export_issue":
            logger.info(
                f"Export issue detected, proceeding to export issue handling.\n```json\n{bug.model_dump_json(indent=2)}\n```"
            )

            return {**state, "status": "export_issue_detected"}
        case _:
            logger.error(
                f"Bug classification failed, stopping processing.\n```json\n{bug.model_dump_json(indent=2)}\n```"
            )
            logger.info("********************************************************")
            logger.info(user_prompt)
            logger.info("********************************************************")
            return {**state, "status": "bug_classification_failed"}
