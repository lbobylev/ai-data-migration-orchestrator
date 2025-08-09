import operator
from typing import Annotated, List, TypedDict

from langgraph.graph import END, StateGraph


class AgentState(TypedDict):
    messages: Annotated[List[str], operator.add]


def node_a(state: AgentState):
    print("Executing Node A")
    return {"messages": ["Hello from Node A"]}


def node_b(state: AgentState):
    print("Executing Node B")
    print(f"Messages so far: {state['messages']}")
    return {"messages": ["Hello from Node B"]}


workflow = StateGraph(AgentState)

workflow.add_node("A", node_a)
workflow.add_node("B", node_b)

workflow.set_entry_point("A")
workflow.add_edge("A", "B")
workflow.add_edge("B", END)

app = workflow.compile()

if __name__ == "__main__":
    initial_state = {"messages": []}
    for event in app.stream(initial_state):
        print(event)
