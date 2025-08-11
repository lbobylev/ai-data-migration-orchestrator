from langgraph.graph import StateGraph, START, END
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain_core.messages import (
    SystemMessage,
    HumanMessage,
)
from tools.bot import bot
from tools.file_utils import select_file, read_excel
from logger import ToolLogger
from typing import Literal, TypedDict
import json
from pydantic import BaseModel, Field, ValidationError

logger = ToolLogger()
tools = [bot, select_file, read_excel]
fast_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
smart_llm = ChatOpenAI(model="gpt-4o", temperature=0.0)

Status = Literal[
    "data_migration_detected",
    "task_classification_failed",
    "base_material_update_detected",
    "data_migration_classification_failed",
    "base_material_material_patch_detected",
    "base_material_patch_classification_failed",
    "base_material_patch_creation_failed",
    "file_selected",
    "schema_validation_passed",
    "file_selection_failed",
    "other",
]


class LibraryEntry(BaseModel):
    key: str = Field(
        description="The key of the library entry, which is the unique identifier."
    )


class BaseMaterialPredicate(BaseModel):
    organizationId: str = Field(description="The organization ID")
    vendorCode: str = Field(
        description="The base material vendor code, which is the key of the base material."
    )


class BaseMaterialPatch(BaseModel):
    predicate: BaseMaterialPredicate = Field(
        description="The predicate for the base material update."
    )
    material: LibraryEntry = Field(
        description="The base material key, which is the key of the base material."
    )


class MyState(TypedDict):
    user_prompt: str
    user_input: str
    status: Status
    task_data: dict | None


def user_input_processing_node(state: MyState) -> MyState:
    system_prompt = """
    You should process the user input and return it as a string.
    The user input should be prepaged for futher classification.
    You should remove any unnecessary information, such as greetings, and focus on the main request.
    You should fix grammatical errors and typos, but do not change the meaning of the request.
    You shoudl make it as much concise as possible, but still keep the main request intact.
    """
    response = fast_llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=state["user_input"]),
        ]
    )

    return {
        **state,
        "user_prompt": str(response.content),
        "user_input": state["user_input"],
    }


def task_classification_node(state: MyState) -> MyState:
    system_prompt = """
    You are an analyst. Classify the user's request.
If the request is about:
    - changing a mapping
    - mentions an attached file
    - involves updating data
    - mentions fields or columns in a file to be updated
classify it as "data_migration_detected". 
Otherwise, classify it as "other".
Return exactly one of: "data_migration_detected", "other".
    """
    response = fast_llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=state["user_prompt"]),
        ]
    )
    status = str(response.content).strip().lower()
    match status:
        case "data_migration_detected":
            print("Data migration detected, proceeding to classification.")
            return {**state, "status": "data_migration_detected"}
        case _:
            print("Task classification failed, stopping processing.")
            return {**state, "status": "task_classification_failed"}


def data_migration_classification_node(state: MyState) -> MyState:
    system_prompt = """
    You are an analyst. Classify the data migration request.
Classify the request. If it involves changing, replacing, remapping, or updating base naterials (e.g., replacing old base material keys with new ones, mapping one set of base materials to another, or updating components based on such changes), output exactly: base_material_update_detected. Otherwise output exactly: other. You must respond "other" or "base_material_update_detected".
"""
    response = smart_llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=state["user_prompt"]),
        ]
    )
    response_content = str(response.content).strip().lower()
    match response_content:
        case "base_material_update_detected":
            print("Base material update detected, proceeding to file selection.")
            return {**state, "status": "base_material_update_detected"}
        case _:
            print("Data migration classification failed, stopping processing.")
            return {**state, "status": "data_migration_classification_failed"}


def file_selection_node(state: MyState) -> MyState:
    file_path = select_file.invoke({})
    if not file_path:
        print("No file selected, stopping processing.")
        return {**state, "status": "file_selection_failed"}
    task_data = state.get("task_data") or {}
    return {
        **state,
        "status": "file_selected",
        "task_data": {**task_data, "file_path": file_path},
    }


def schema_validation_node(state: MyState) -> MyState:
    return {**state, "status": "schema_validation_passed"}


def base_material_update_node(state: MyState) -> MyState:
    predicate_prompt = """
You are given a JSON object containing vendor and material information. Your task is to extract and return a simplified JSON object in the following format:  
{
  "organizationId": "<Vendor Code value>",
  "vendorCode": "<Base Material Vendor Code value>"
}

Rules:  
1. Keys in the input JSON may vary in naming. Treat any of the following as possible vendor keys:  
   "Vendor Code", "vendor_code", "vendor", "organization", "vendorName", "vendor_id", "org".  
   Treat any of the following as possible base material vendor code keys:  
   "Base Material Vendor Code", "base_material_vendor_code", "materialVendor", "material_vendor_code", "material_vendor".  

2. organizationId should be determined from the vendor value in the input (case-insensitive).  
   - Use the organization list below for context. If the value matches (exact match ignoring case) one of these organizations, keep it as-is.  
   - If it doesn’t match, still return it as-is.  

3. vendorCode should be determined from the base material vendor code value in the input, regardless of exact key name.  

4. Output must be a valid JSON object containing only organizationId and vendorCode.  
   - No explanations, no extra keys, no code blocks.  

Example:  
Input:  
{
    "vendor": "Barberini",
    "material_vendor_code": "PA",
    "otherData": "ignore"
}  

Output:  
{
    "organizationId": "barberini",
    "vendorCode": "PA"
}  
"""

    task_data = state.get("task_data") or {}

    return {**state, "task_data": {**task_data, "predicate_prompt": predicate_prompt}}


def base_material_patch_classification_node(state: MyState) -> MyState:
    system_prompt = """
You are a classifier that determines the type of request based on user input.  
If the request explicitly mentions "KEYE key" in relation to a base material (e.g., OLD Base Material KEYE Key, NEW Base Material KEYE Key, or similar), return exactly:
base_material_material_patch_detected
Otherwise, return exactly:
other
Return strictly one of these two lines.
"""
    response = smart_llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=state["user_prompt"]),
        ]
    )
    response_content = str(response.content).strip().lower()
    match response_content:
        case "base_material_material_patch_detected":
            return {**state, "status": "base_material_material_patch_detected"}
        case _:
            print("Base material patch classification failed, stopping processing.")
            return {**state, "status": "base_material_patch_classification_failed"}


def base_material_patch_node(state: MyState) -> MyState:
    task_data = state.get("task_data") or {}
    predicate_prompt = task_data.get("predicate_prompt")
    if not predicate_prompt:
        print("No predicate prompt found, stopping processing.")
        return {**state, "status": "other"}
    status = state.get("status")
    match status:
        case "base_material_material_patch_detected":
            file_path = task_data.get("file_path")
            rows = read_excel.invoke({"file_path": file_path})
            for row in rows:
                create_base_material_material_patch(predicate_prompt, row)

            return {**state, "status": "other"}
        case _:
            print("Base material patch creation failed, stopping processing.")
            return {**state, "status": "base_material_patch_creation_failed"}


def create_base_material_material_patch(predicate_prompt: str, row: dict) -> None:
    system_prompt = (
        """
You will receive two separate system prompts:
Predicate Prompt – This instructs you how to create a predicate.
Material Key Prompt – This instructs you how to extract the base material key from a JSON object.
Perform both actions exactly as each system prompt describes.
Use the Predicate Prompt to produce a string output (the predicate result).
Use the Material Key Prompt to find the required base material key in the provided JSON and return its value (the base material key result).
Finally, return your results in one JSON object with the following structure:
{
  "predicate": "<predicate result>",
  "material": {
    "key": "<base material key result>"
  }
"""
        f"""
Predicate prompt:
{predicate_prompt}"""
        """
Material key prompt:
Given a JSON object, find the value of the key whose name contains all of the following words (case-insensitive): 
"new", "keye", "key", and "base material". 
If multiple keys match, return the first match. 
If no key matches, return null.

Example input:
{
    "Vendor Code": "barberini",
    "Base Material Vendor Code": "PA",
    "Base Material Vendor Description": "Nylon",
    "Material Family Code": "PLA",
    "Material Family Description": "Plastic",
    "Base Material Certification Uploaded": "No",
    "OLD Base Material KEYE Key": "Plastic - nylon - conventional",
    "Mandatory Certification Type Code 1": NaN,
    "Mandatory Certification Type Description 1": NaN,
    "Validation": "ACCEPTED",
    "NEW Base Material KEYE Key": "Plastic - nylon - conventional - Grilamid TR XE 3805"
}
Expected output:
"Plastic - nylon - conventional - Grilamid TR XE 3805"
"""
    )

    struct_llm = smart_llm.with_structured_output(BaseMaterialPatch, method="json_mode")
    patch = struct_llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=json.dumps(row, ensure_ascii=False)),
        ]
    )

    print(patch)


def route_by_status(state: MyState) -> str:
    match state.get("status"):
        case "data_migration_detected":
            return "data_migration_classification_node"
        case "base_material_update_detected":
            return "file_selection_node"
        case "file_selected":
            return "schema_validation_node"
        case "schema_validation_passed":
            return "base_material_update_node"
        case "base_material_material_patch_detected":
            return "base_material_patch_node"

    print("Task is not recognized, routing to END.")
    return END


graph = StateGraph(MyState)
graph.add_node("user_input_processing_node", user_input_processing_node)
graph.add_node("task_classification_node", task_classification_node)
graph.add_node("data_migration_classification_node", data_migration_classification_node)
graph.add_node("base_material_update_node", base_material_update_node)
graph.add_node("file_selection_node", file_selection_node)
graph.add_node("schema_validation_node", schema_validation_node)
graph.add_node(
    "base_material_patch_classification_node", base_material_patch_classification_node
)
graph.add_node("base_material_patch_node", base_material_patch_node)
graph.add_edge(START, "user_input_processing_node")
graph.add_edge("user_input_processing_node", "task_classification_node")
graph.add_conditional_edges(
    "task_classification_node",
    route_by_status,
    ["data_migration_classification_node", END],
)
graph.add_conditional_edges(
    "data_migration_classification_node",
    route_by_status,
    ["file_selection_node", END],
)
graph.add_conditional_edges(
    "file_selection_node",
    route_by_status,
    ["schema_validation_node", END],
)
graph.add_conditional_edges(
    "schema_validation_node",
    route_by_status,
    ["base_material_update_node", END],
)
graph.add_edge("base_material_update_node", "base_material_patch_classification_node")
graph.add_conditional_edges(
    "base_material_patch_classification_node",
    route_by_status,
    ["base_material_patch_node", END],
)
graph.add_edge("base_material_patch_node", END)
app = graph.compile()

user_input = """
We’d need to change the mapping of the base materials listed in the attached file.

Expected result: substitute the “OLD Base Material KEYE Key” (column G in the attached file) with “NEW Base Material KEYE Key” (column K in the attached file).
We expect that all components that have one or more of the base materials mentioned in the attached file will be consequently updated.
"""


def dump(x):
    print(json.dumps(x, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    app.invoke(
        {"user_input": user_input, "status": "other"},
        config={"callbacks": [logger], "recursion_limit": 30},
    )
