from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph

from app_types import (
    DataMigration,
    MyState,
)
from confirm import set_confirm_disable
from github_utils import get_issue, get_issues
from logger import GraphLogger, get_logger
from nodes.bug_classification_node import make_bug_classification_node
from nodes.data_extraction_node import make_data_extraction_node
from nodes.data_migration_classification_node import (
    make_data_migration_classification_node,
)
from nodes.file_download_node import file_download_node
from nodes.operation_plan_fanout_node import make_operation_plan_fanout_node
from nodes.operation_plan_init_node import make_operation_plan_init_node
from nodes.operation_worker_node import make_operation_worker_node
from nodes.task_classification_node import make_task_classification_node
from nodes.user_input_processing_node import make_user_input_processing_node
from nodes.operation_gather_node import operation_gather_node
import asyncio

load_dotenv()
graphLogger = GraphLogger()
logger = get_logger()
fast_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
smart_llm = ChatOpenAI(model="gpt-4o", temperature=0.0)

set_confirm_disable(True)


def route_by_status(state: MyState) -> str:
    match state.get("status"):
        case "data_migration_detected":
            return "data_migration_classification_node"
        case "bug_detected":
            return "bug_classification_node"
        case "data_migration_classified":
            return "operation_plan_init_node"
    logger.error("Task is not recognized, routing to END.")
    return END


def route_by_data_source(state: MyState) -> str:
    task = state.get("task")
    if isinstance(task, DataMigration):
        if task.data_source == "attachment_file":
            return "file_download_node"
        elif task.data_source == "user_request":
            return "data_extraction_node"
    logger.error("Data source is not recognized, routing to END.")
    return END


operation_plan_fanout_node = make_operation_plan_fanout_node(smart_llm)


graph = StateGraph(MyState)
graph.add_node("user_input_processing_node", make_user_input_processing_node(fast_llm))
graph.add_node("task_classification_node", make_task_classification_node(fast_llm))
graph.add_node("bug_classification_node", make_bug_classification_node(fast_llm))
graph.add_node(
    "data_migration_classification_node",
    make_data_migration_classification_node(fast_llm),
)
graph.add_node("operation_plan_init_node", make_operation_plan_init_node(fast_llm))
graph.add_node(
    "operation_plan_fanout_node",
    operation_plan_fanout_node,
)
graph.add_node(
    "operation_worker_node",
    make_operation_worker_node(fast_llm),
)
graph.add_node(
    "operation_gather_node",
    operation_gather_node,
)
graph.add_node("file_download_node", file_download_node)
graph.add_node("data_extraction_node", make_data_extraction_node(fast_llm))
graph.add_edge(START, "user_input_processing_node")
graph.add_edge("user_input_processing_node", "task_classification_node")
graph.add_conditional_edges(
    "task_classification_node",
    route_by_status,
    ["data_migration_classification_node", "bug_classification_node", END],
)
graph.add_edge("bug_classification_node", END)
graph.add_conditional_edges(
    "data_migration_classification_node",
    route_by_status,
    ["operation_plan_init_node", END],
)
graph.add_conditional_edges(
    "operation_plan_init_node",
    route_by_data_source,
    ["file_download_node", "data_extraction_node", END],
)
graph.add_conditional_edges("file_download_node", operation_plan_fanout_node)
graph.add_conditional_edges("data_extraction_node", operation_plan_fanout_node)
graph.add_edge("operation_plan_fanout_node", "operation_worker_node")
graph.add_edge("operation_worker_node", "operation_gather_node")
graph.add_conditional_edges(
    "operation_gather_node",
    lambda s: END if s["done"] >= s["total"] else "operation_gather_node",
)
app = graph.compile()


async def main():
    issues = get_issues()[0:1]

    await app.abatch(
        [{"issue": issue, "status": "other"} for issue in issues],
        config={"callbacks": [graphLogger], "recursion_limit": 30},
    )


if __name__ == "__main__":
    #asyncio.run(main())
    logger.info("Starting the application...")
    issue_num = 679  # 676 # 607
    issue = get_issue(issue_num)
    app.invoke(
        {"issue": issue, "status": "other"},
        config={"callbacks": [graphLogger], "recursion_limit": 30},
    )
