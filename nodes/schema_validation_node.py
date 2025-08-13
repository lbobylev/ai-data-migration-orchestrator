from app_types import MyState


def schema_validation_node(state: MyState) -> MyState:
    return {**state, "status": "schema_validation_passed"}
