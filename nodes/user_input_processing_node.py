from typing import get_args
from langchain_openai import ChatOpenAI
from app_types import ENVIRONMENTS, AssetType, MyState
from logger import get_logger
from llm_utils import retry_call
from langchain_core.prompts import ChatPromptTemplate

logger = get_logger(__name__)
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0, top_p=1)


def user_input_processing_node(state: MyState) -> MyState:
    logger.info("Processing user input...")

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You should process the user input and return it as a string.
The user input should be prepaged for futher classification.
You should remove any unnecessary information, such as greetings, and focus on the main request.
You should fix grammatical errors and typos, but do not change the meaning of the request.
You shoudl make it as much concise as possible, but still keep the main request intact.
You should keep all the urls and other important information that can be used for further processing.
You must keep the environments if mentioned in the body or in the title.
Be careful and do not remove any environments if mentioned.
Possbile environments are: {envs}.
Be carefull and keep all the asset types if mentioned in the body or in the title.
Possible asset types are: {asset_types}.
    """,
            ),
            ("user", "{user_input}"),
        ]
    )

    user_input = state.get("user_input")
    if not user_input:
        logger.error("User input is empty or not provided.")
        return state

    try:
        response = retry_call(prompt | llm).invoke(
            {
                "user_input": user_input,
                "envs": ", ".join(ENVIRONMENTS),
                "asset_types": ", ".join(get_args(AssetType)),
            }
        )

        user_prompt = str(response.content)
        logger.debug(f"{user_prompt}")
        logger.info("User input processed successfully.")
        return {**state, "user_prompt": user_prompt}
    except Exception as e:
        logger.error(f"Error processing user input: {e}")
        return {**state, "status": "user_input_processing_failed"}
