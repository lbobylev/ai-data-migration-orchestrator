import json
from typing import Any, Dict, List, Tuple

from langchain_core.messages import (
    HumanMessage,
    SystemMessage,
)
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, model_validator

from app_types import (
    AssetFieldSpec,
    AssetPatch,
    AssetSpec,
    AssetType,
    LibraryEntry,
    Operation,
)
from confirm import require_confirm
from logger import get_logger
from llm_utils import call_with_self_heal
import numpy as np

logger = get_logger(__name__)

PatchFieldMapping = Tuple[str, str | AssetFieldSpec]


class ExecutionTask(BaseModel):
    asset_type: AssetType
    operation: Operation
    patches: List[AssetPatch]


class AssetMapping(BaseModel):
    predicate: List[Tuple[str, str]]
    patch: List[PatchFieldMapping]


def unflatten(data: Dict[str, str]) -> Dict[str, Any]:
    """
    Unflattens a dictionary with dot notation keys into a nested dictionary.
    """
    result = {}
    for key, value in data.items():
        parts = key.split(".")
        current = result
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result


@require_confirm()
def _create_asset_mapping(
    llm: ChatOpenAI,
    asset_spec: AssetSpec,
    example: Dict[str, str],
) -> AssetMapping:
    system_prompt = """
You will be given:

1. A **spec JSON object** defining mappings between input field names and output field names:

{
    "fields": {<input_field1>: <output_field2>, ...},
    "predicate_fields": [<list of output field names to be used as predicates>]
}

2. An **example input record** with field names and values.
   - Input field names may not exactly match the spec but will usually be similar.
   - Fields representing **updates/patches** often contain "new" or "updated" or "review".
   - Fields representing **old/previous values** often contain "old" or "previous" and must be **ignored**.

---

### Your task:
Return a JSON object in the form:

{
    "predicate": [
        (<input_field_name>, <output_field_name>),
        ...
    ],
    "patch": [
        (<input_field_name>, <output_field_name>),
        ...
    ]
}

---

### Rules:

1. **Do not alter field names.**
   - Always use field names exactly as they appear in the input record and the spec.

2. **Do not split or truncate field names.**
   - If a field name contains dots (e.g., "user.address.street"), treat it as a single name.

3. **Predicate vs Patch:**
   - If the mapped output field is in `predicate_fields`, include it under `"predicate"`.
   - Otherwise, if the input field is an update/patch field (`new_*`, `updated_*`, etc.), include it under `"patch"`.

4. **Ignore old/previous fields.**
   - Any input field containing "old" or "previous" must be excluded.

5. **Important:**  
   - Sometimes an output field is not a string but a **spec object**, e.g.  
     `{"name": "types", "type": "library_entry_list"}`  
   - In such cases, use the **entire object** in the output mapping.
"""

    spec = asset_spec.model_dump_json(indent=2, exclude_none=True)
    example_input = json.dumps(example)

    user_prompt = f"Example input:\n{example_input}\nField specification:\n{spec}\n\nPlease do the mapping."

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]

    class Response(BaseModel):
        predicate: List[Tuple[str, str]]
        patch: List[PatchFieldMapping]

        @model_validator(mode="after")
        def _validate_mapping(self):
            predicate = self.predicate
            patch = self.patch

            input_fields = example.keys()
            patch_input_fields = [x[0] for x in patch]
            output_fields = asset_spec.fields.values()
            predicate_output_fields = [x[1] for x in predicate]
            patch_output_fields = [x[1] for x in patch]

            logger.info(
                f"Asset spec fields:\n```json\n{asset_spec.model_dump_json(indent=2)}\n```"
            )

            for f in predicate_output_fields:
                if f not in output_fields:
                    raise ValueError(
                        f"Predicate output field '{f}' not found in spec fields"
                    )
            for f in patch_output_fields:
                if f not in output_fields:
                    raise ValueError(
                        f"Patch output field '{f}' not found in spec fields: {json.dumps(output_fields)}"
                    )
            for f in patch_input_fields:
                if f not in input_fields:
                    raise ValueError(
                        f"Patch input field '{f}' not found in example input fields"
                    )
            return self

    response = call_with_self_heal(messages, llm, Response)

    logger.info(
        f"Asset mapping response:\n```json\n{response.model_dump_json(indent=2)}\n```"
    )

    return AssetMapping.model_validate(response.model_dump())


def _identify_updatable_fields(llm: ChatOpenAI, task_description: str) -> List[str]:
    """
    For a given task description with example input, identify which fields are intended for updating.
    """
    system_prompt = """
You will be given:  
1. A **task description/context** describing the update operation.
2. An **example input record** with field names and values.
Your task is to identify which fields in the example input are intended for updating based on the task description.

### Output  
Return exactly the JSON object in the following structure:
{
  "results": [<list of field names to be updated>]
}
"""
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content="Task description/context:\n" + task_description),
    ]

    class Response(BaseModel):
        results: List[str]

    fields = call_with_self_heal(messages, llm, Response).results

    return fields


def _run_mapping_with_specs(
    input_data: List[Dict[str, str]], asset_mapping: AssetMapping
) -> List[AssetPatch]:
    """
    Maps input data using the provided mapping object.
    """

    def item_to_patch(record: Dict[str, str]) -> AssetPatch:
        try:
            predicate = {
                out_field: record[in_field]
                for in_field, out_field in asset_mapping.predicate
            }

            patch = {}
            for in_field, out_field in asset_mapping.patch:
                value = record.get(in_field, "").strip()
                if isinstance(out_field, dict):
                    field_name = out_field.get("name")
                    patch[field_name] = {**out_field, "value": value}
                else:
                    patch[out_field] = value

            return AssetPatch(predicate=predicate, patch=patch)
        except KeyError as e:
            raise ValueError(f"Missing expected field in input record: {e}")

    return [item_to_patch(x) for x in input_data]


def _resolve_patch_specs(llm: ChatOpenAI, patches: List[dict]) -> List[dict]:
    system_prompt = """
You will be given a **list of patch objects**.  

The fields in each patch follow a special **asset field spec object** format:

{
  "name": "<name_of_the_field>",
  "type": "<type_of_the_field>",
  "array_value_type": "<type_of_array_values>" (optional),
  "value": "<the actual value to set>"
}

Your task is to **convert all asset field spec objects into their proper values**.  
**Important:** The length of the output list must exactly match the input list, and the keys in each patch object must remain unchanged.

### Conversion Rules

1. **Primitive Types**  
   - Convert values based on their declared "type".  
   - Example:  
     - For "type": "boolean", convert "Yes", "True", "Active" to true, and "No", "False", "Inactive" to false.  
     - Be careful with *negated meanings*:  
       - If the field name suggests negation (e.g., "disabled", "not active"), then "Yes"/"True" should become true (meaning the negation is active).

2. **Special Handling for Booleans**  
   - Field names like "disabled", "inactive", "not_available" invert the intuitive mapping.  
     - Example: "value": "Not Active" with "name": "disabled" ⇒ true.

3. **Array Fields**  
   - If "type": "array" and "array_value_type" is "LibraryEntry", and the provided value is a string, convert it to:  
     { "id": "<value>" }

4. **LibraryEntry Objects**  
   - For fields of type "LibraryEntry" (not in an array), convert the value to:
     { "id": "<value>" }
   - If the input already has {"id": "...", "code": "..."}, leave it unchanged.

5. **Nullability**
    - If a field is marked as nullable and the value is empty or "None", set it to null.
    - If a field is nullable and the value is "nan" or similar, treat it as null.

**Important:** You must replace the spect object with the converted value, not nest it.
Example:
{
  "semiFinishedSupplier": {
    "name": "semiFinishedSupplier",
    "type": "boolean",
    "value": "Yes"
  },
  "types": {
    "name": "types",
    "type": "array",
    "array_value_type": "LibraryEntry",
    "value": "Component/Raw Material Supplier"
  },
  "catalogUploadedBy": {
    "name": "catalogUploadedBy",
    "type": "string",
    "nullable": true,
    "value": "None"
  }
}
Should be converted to:
{
  "semiFinishedSupplier": true,
  "types": [
    { "id": "Component/Raw Material Supplier" }
  ],
  "catalogUploadedBy": null
}
---

### Output
Return the result in the following structure:

{
  "results": [
    <converted_patch_1>,
    <converted_patch_2>,
    ...
  ]
}
"""
    smart_llm = ChatOpenAI(model="gpt-4o", temperature=0)

    n_patches = len(patches)
    resolved_values = []
    chunks = np.array_split(np.asarray(patches), max(1, n_patches // 100))
    logger.info(f"Resolving {n_patches} patches in {len(chunks)} chunks")
    for chunk in chunks:
        chunk_size = len(chunk)

        class Response(BaseModel):
            results: List[Dict[str, Any]] = Field(
                min_length=chunk_size, max_length=chunk_size
            )

            @model_validator(mode="after")
            def _validate_response(self):
                for i, result in enumerate(self.results):
                    if set(result.keys()) != set(patches[i].keys()):
                        raise ValueError(
                            f"Keys mismatch in patch {i}: expected {set(patches[i].keys())}, got {set(result.keys())}"
                        )
                    if set(result.keys()) != set(patches[i].keys()):
                        raise ValueError(
                            f"Keys mismatch in patch {i}: expected {set(patches[i].keys())}, got {set(result.keys())}"
                        )
                    patch = patches[i]
                    for key, spec in patch.items():
                        field_type = spec.get("type")
                        val = result[key]
                        match field_type:
                            case "array":
                                if not isinstance(result[key], list):
                                    raise ValueError(
                                        f"Expected list for field '{key}', got {type(result[key])}\nValue:\n```json\n{json.dumps(result[key], indent=2)}\n```"
                                    )
                                array_value_type = spec.get("array_value_type")
                                if array_value_type == "LibraryEntry":
                                    for x in val:
                                        LibraryEntry.model_validate(x)
                            case "boolean":
                                if not isinstance(result[key], bool):
                                    raise ValueError(
                                        f"Expected boolean for field '{key}', got {type(result[key])}\nValue:\n```json\n{json.dumps(result[key], indent=2)}\n```"
                                    )
                            case "string":
                                if result[key] is not None and not isinstance(
                                    result[key], str
                                ):
                                    raise ValueError(
                                        f"Expected string or null for field '{key}', got {type(result[key])}\nValue:\n```json\n{json.dumps(result[key], indent=2)}\n```"
                                    )
                            case _:
                                ...
                return self

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(
                content="Please convert the following patch:\n"
                + json.dumps(chunk.tolist(), indent=2)
            ),
        ]

        logger.info(f"Processing chunk with {len(chunk)} patches")

        chunk_results = call_with_self_heal(
            messages, smart_llm, Response, max_repairs=5
        ).results

        logger.info(
            f"Chunk processed:\n```json\n{json.dumps(chunk_results, indent=2)}\n```"
        )

        resolved_values.extend(chunk_results)

    return resolved_values


def _skip_non_updatable_fields(
    llm: ChatOpenAI,
    asset_mapping: AssetMapping,
    task_description: str,
    example: Dict[str, str],
):
    logger.info("Skipping non-updatable fields for update operation")
    task_description += "\n\n" + json.dumps(example, indent=2)
    updatable_fields = _identify_updatable_fields(llm, task_description)
    logger.info(f"Identified updatable fields: {updatable_fields}")
    asset_mapping.patch = [
        (in_field, out_field)
        for in_field, out_field in asset_mapping.patch
        if in_field in updatable_fields
    ]
    remaining_patch_fields = json.dumps(asset_mapping.patch, indent=2)
    logger.info(
        f"Skipping non-updatable fields completed.\nRemaining fields:\n```json\n{remaining_patch_fields}\n```"
    )


def create_patches(
    llm: ChatOpenAI,
    *,
    asset_type: AssetType,
    operation_name: Operation,
    asset_spec: AssetSpec,
    input_data: List[Dict[str, str]],
    task_description: str,
) -> List[AssetPatch]:
    if len(input_data) == 0:
        raise ValueError("No input data found for the operation")

    example_input = input_data[0]

    logger.info(f"Creating asset mapping for {asset_type} {operation_name}")
    asset_mapping = _create_asset_mapping(llm, asset_spec, example_input)

    invalid_predicate = ValueError(
        f"Mapping function must include all predicate fields: {asset_spec.predicate_fields}, got: {asset_mapping.predicate}"
    )

    if operation_name == "update":
        if len(asset_mapping.predicate) != len(asset_spec.predicate_fields):
            raise invalid_predicate
        if len(asset_mapping.patch) == 0:
            raise ValueError("Mapping function must include at least one patch field")

        _skip_non_updatable_fields(llm, asset_mapping, task_description, example_input)

    elif operation_name == "delete":
        if len(asset_mapping.predicate) != len(asset_spec.predicate_fields):
            raise invalid_predicate

    logger.info(f"Mapping data started for {asset_type} {operation_name}")
    mapped_data_with_specs = _run_mapping_with_specs(input_data, asset_mapping)
    logger.info(f"Mapping data completed for {asset_type} {operation_name}")

    specs_from_patches = [
        {k: v for k, v in x.patch.items() if "name" in v}
        for x in mapped_data_with_specs
    ]

    # logger.info(f"Specs extracted:\n```json\n{json.dumps(specs_from_patches, indent=2)}\n```")

    if any(len(x) > 0 for x in specs_from_patches):
        logger.info("Patch conversion started")
        results = _resolve_patch_specs(llm, specs_from_patches)
        for i, result in enumerate(results):
            patch = mapped_data_with_specs[i].patch
            mapped_data_with_specs[i].patch = {**patch, **result}
        logger.info("Patch conversion completed")

    if operation_name == "create":
        for x in mapped_data_with_specs:
            x.patch = {**x.patch, **x.predicate}

    logger.info("Unflattening mapped data")
    for x in mapped_data_with_specs:
        x.patch = unflatten(x.patch)
    logger.info("Unflattening completed")

    return mapped_data_with_specs
