from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from app_types import MyState
from logger import get_logger
from llm_utils import retry_call

logger = get_logger()


def make_user_input_processing_node(llm: ChatOpenAI):
    def user_input_processing_node(state: MyState) -> MyState:
        system_prompt = """
        You should process the user input and return it as a string.
        The user input should be prepaged for futher classification.
        You should remove any unnecessary information, such as greetings, and focus on the main request.
        You should fix grammatical errors and typos, but do not change the meaning of the request.
        You shoudl make it as much concise as possible, but still keep the main request intact.
        You should keep all the urls and other important information that can be used for further processing.
        You must keep the environments if mentioned in the user input.
        """

        issue = state.get("issue")
        if not issue:
            logger.error("Issue is not provided in the state.")
            return state

        user_input = f"{issue["title"]}\n{issue["body"]}"

        response = retry_call(
            lambda: llm.invoke(
                [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=user_input),
                ]
            )
        )

        user_prompt = str(response.content)
        logger.info(f"Processed user input:\n\n{user_prompt}")

        return {
            **state,
            "user_prompt": user_prompt 
        }

    return user_input_processing_node
