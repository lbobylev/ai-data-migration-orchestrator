
from langchain_openai import ChatOpenAI
from app_types import (
    MyState,
)


def make_operation_plan_init_node(llm: ChatOpenAI):
    def operation_plan_init_node(state: MyState) -> MyState:
        ops = state.get("detected_operations") or []

        return {
            **state,
            "results": [],
            "errors": [],
            "total": len(ops),
            "done": 0,
        }

    return operation_plan_init_node
