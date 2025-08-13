from typing import Any, Optional
from uuid import UUID
from dotenv import load_dotenv
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage, HumanMessage
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph, MessagesState
from langgraph.prebuilt import ToolNode, tools_condition
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.patch_stdout import patch_stdout

from github_utils import get_issue

from app_types import (
    GithubIssue,
    MyState,
)
from confirm import set_confirm_disable
from logger import AppLogger, get_logger
from nodes.asset_type_detection_node import asset_type_detection_node
from nodes.bug_classification_node import bug_classification_node
from nodes.data_extraction_node import data_extraction_node
from nodes.data_source_detection_node import data_source_detection_node
from nodes.delete_notifications_node import delete_notifications_node
from nodes.environment_detection_node import environment_detction_node
from nodes.file_download_node import file_download_node
from nodes.operation_detection_node import operation_detection_node
from nodes.patch_extraction_node import patch_extraction_node
from nodes.task_classification_node import task_classification_node
from nodes.task_execution_node import task_execution_node
from nodes.user_input_processing_node import user_input_processing_node
from nodes.supplier_library_entry_deprecation_node import (
    supplier_library_entry_deprecation_node,
)
from nodes.supplier_library_entry_operation_detection_node import (
    supplier_library_entry_operation_detection_node,
)
from nodes.eyewear_manufacturer_assignment_operation_detection_node import (
    eyewear_manufacturer_assignment_operation_detection_node,
)
from nodes.supplier_library_entry_creation_node import (
    supplier_library_entry_creation_node,
)
from nodes.delete_organization_by_id_node import delete_organization_by_id_node
from nodes.task_creation_node import task_creation_node
from tasks import tasks

load_dotenv()
app_logger = AppLogger()
logger = get_logger(__name__)

set_confirm_disable(True)


def route_by_status(state: MyState) -> str:
    match state.get("status"):
        case "data_migration_detected":
            return "asset_type_detection_node"
        case "bug_detected":
            return "bug_classification_node"
        case "delete_notifications_detected":
            return "environment_detection_node"
        case "delete_organization_by_id_detected":
            return "environment_detection_node"
        case "data_migration_classified":
            return "operation_detection_node"
        case "operation_detected":
            return "operation_plan_init_node"

    logger.error("Task is not recognized, routing to END.")
    return END


def route_by_data_source(state: MyState) -> str:
    data_source = state.get("data_source")
    if data_source == "attachment_file":
        return "file_download_node"
    elif data_source == "user_request":
        return "data_extraction_node"
    return END


def route_after_data_extraction(state: MyState) -> str:
    status = state.get("status")
    if status == "no_tabular_data_found":
        return "operation_detection_node"

    asset_type = state.get("asset_type")
    match asset_type:
        case "SupplierLibraryEntry":
            return "supplier_library_entry_operation_detection_node"
        case "EyewearManufacturerAssignment":
            return "eyewear_manufacturer_assignment_operation_detection_node"
        case _:
            return "operation_detection_node"


graph = StateGraph(MyState)
graph.add_node("bug_classification_node", bug_classification_node)
graph.add_node("delete_notifications_node", delete_notifications_node)
graph.add_edge("delete_notifications_node", END)
graph.add_edge("bug_classification_node", END)

graph.add_node("delete_organization_by_id_node", delete_organization_by_id_node)
graph.add_node("user_input_processing_node", user_input_processing_node)
graph.add_node("task_classification_node", task_classification_node)
graph.add_node("asset_type_detection_node", asset_type_detection_node)
graph.add_node("environment_detection_node", environment_detction_node)
graph.add_node("data_source_detection_node", data_source_detection_node)
graph.add_node("file_download_node", file_download_node)
graph.add_node("data_extraction_node", data_extraction_node)
graph.add_node("operation_detection_node", operation_detection_node)
graph.add_node("patch_extraction_node", patch_extraction_node)
graph.add_node(
    "supplier_library_entry_operation_detection_node",
    supplier_library_entry_operation_detection_node,
)
graph.add_node(
    "eyewear_manufacturer_assignment_operation_detection_node",
    eyewear_manufacturer_assignment_operation_detection_node,
)
graph.add_node(
    "supplier_library_entry_deprecation_node",
    supplier_library_entry_deprecation_node,
)
graph.add_node("task_creation_node", task_creation_node)
graph.add_node("task_execution_node", task_execution_node)
graph.add_node(
    "supplier_library_entry_creation_node", supplier_library_entry_creation_node
)

graph.add_edge(START, "user_input_processing_node")
graph.add_edge("user_input_processing_node", "task_classification_node")

graph.add_conditional_edges(
    "task_classification_node",
    route_by_status,
    [
        "asset_type_detection_node",
        "environment_detection_node",
        "bug_classification_node",
        END,
    ],
)

graph.add_edge("asset_type_detection_node", "environment_detection_node")
graph.add_conditional_edges(
    "environment_detection_node",
    lambda s: (
        "data_source_detection_node"
        if s.get("task_type") == "data_migration"
        else (
            "delete_notifications_node"
            if s.get("task_type") == "delete_notifications"
            else END
        )
    ),
    [
        "delete_notifications_node",
        "delete_organization_by_id_node",
        "data_source_detection_node",
        END,
    ],
)
graph.add_conditional_edges(
    "data_source_detection_node",
    route_by_data_source,
    ["file_download_node", "data_extraction_node", END],
)
graph.add_conditional_edges(
    "file_download_node",
    route_after_data_extraction,
    [
        "operation_detection_node",
        "supplier_library_entry_operation_detection_node",
        "eyewear_manufacturer_assignment_operation_detection_node",
    ],
)
graph.add_conditional_edges(
    "data_extraction_node",
    route_after_data_extraction,
    [
        "operation_detection_node",
        "supplier_library_entry_operation_detection_node",
        "eyewear_manufacturer_assignment_operation_detection_node",
    ],
)
graph.add_conditional_edges(
    "supplier_library_entry_operation_detection_node",
    lambda s: (
        "supplier_library_entry_deprecation_node"
        if s.get("detected_operation") == "deprecation"
        else (
            "supplier_library_entry_creation_node"
            if s.get("detected_operation") == "create"
            else "task_creation_node"
        )
    ),
    [
        "task_creation_node",
        "supplier_library_entry_deprecation_node",
        "supplier_library_entry_creation_node",
    ],
)
graph.add_edge("task_creation_node", "task_execution_node")
graph.add_edge(
    "eyewear_manufacturer_assignment_operation_detection_node",
    "task_execution_node",
)
graph.add_edge("task_execution_node", END)
graph.add_edge("supplier_library_entry_deprecation_node", END)
graph.add_edge("supplier_library_entry_creation_node", END)
graph.add_edge("patch_extraction_node", END)

app = graph.compile()


def issue_to_str(issue: GithubIssue) -> str:
    n = issue.get("number") or 0
    title = issue.get("title") or ""
    body = issue.get("body") or ""
    return f"""Number: {n}\nTitle: {title}\nBody: {body}"""


def fetch_issue_from_github(issue_num: int) -> str:
    """
    Fetches issue details from GitHub or a predefined task list based on the issue number.
    """
    if str(issue_num) in tasks:
        return tasks[str(issue_num)]
    issue = get_issue(issue_num)
    return issue_to_str(issue)


@tool
def process_github_issue(issue_number: int) -> str:
    """
    Processes the issue text through the state graph application.
    """
    issue_text = fetch_issue_from_github(issue_number)
    app_invoke(
        {"user_input": issue_text},
    )
    return "done"


@tool
def process_text(user_input: str) -> str:
    """
    Processes the user input text through the state graph application.
    """
    app_invoke(
        {"user_input": user_input},
    )
    return "done"


TOOLS = [process_github_issue, process_text]


llm_with_tools = ChatOpenAI(model="gpt-4o-mini", temperature=0, top_p=1).bind_tools(
    TOOLS
)


def agent(state: MessagesState):
    response = llm_with_tools.invoke(state["messages"])
    return {"messages": [response]}


class BotLogger(BaseCallbackHandler):
    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        inputs: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        print(f"[TOOL START] {serialized['name']} with input:\n{input_str[:300]}...\n")

    def on_tool_end(
        self,
        output: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> Any:
        print(f"[TOOL END] Output:\n{output[:300]}...\n")


def app_invoke(payload, config=None):
    return app.invoke(
        payload,
        config=(
            {"callbacks": [BotLogger()], "recursion_limit": 30}
            if config is None
            else config
        ),
    )


bot_graph = StateGraph(MessagesState)
bot_graph.add_node("agent", agent)
bot_graph.add_node("tools", ToolNode(TOOLS))
bot_graph.add_edge(START, "agent")
bot_graph.add_conditional_edges("agent", tools_condition, {"tools": "tools", END: END})
bot_graph.add_edge("tools", "agent")
bot = bot_graph.compile()


session = PromptSession(
    history=FileHistory(".bot_history"),
)


if __name__ == "__main__":
    print("Surge Agent is ready to assist you. (Type 'exit' to quit)")
    while True:
        try:
            # Prevent background prints (logs/streaming) from garbling the cursor line
            with patch_stdout():
                user = session.prompt(
                    "You: ",
                    auto_suggest=AutoSuggestFromHistory(),
                ).strip()
            if not user:
                continue
            if user.lower() in {"exit", "quit"}:
                raise KeyboardInterrupt
            events = bot.stream(
                {"messages": [HumanMessage(content=user)]},
                stream_mode="values",
                config={"callbacks": [BotLogger()], "recursion_limit": 30},
            )
            last_ai: Optional[AIMessage] = None
            for ev in events:
                # ev is {"messages": [<Message>]} updates from agent/tools
                for m in ev["messages"] or []:
                    if isinstance(m, AIMessage):
                        text = str(m.content)
                        if len(text.strip()) == 0:
                            continue
                        last_ai = m
                        print(
                            f"Agent: {last_ai.content if last_ai else '(no response)'}",
                        )
        except KeyboardInterrupt:
            print("Exiting Surge Agent. Goodbye!")
            break
