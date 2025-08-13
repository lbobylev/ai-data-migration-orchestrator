import json
from typing import Any, Dict, List, Tuple

from langchain_core.messages import (
    HumanMessage,
    SystemMessage,
)
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from app_types import (
    AssetFieldSpec,
    AssetOperation,
    AssetPatch,
    AssetSpec,
    DataMigration,
)
from confirm import require_confirm
from logger import get_logger
from llm_utils import call_with_self_heal

logger = get_logger()


class AssetMapping(BaseModel):
    predicate: List[Tuple[str, str]]
    patch: List[Tuple[str, str | AssetFieldSpec]]


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
    operation: AssetOperation,
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
   - Fields representing **updates/patches** often contain "new" or "updated".
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

    asset_mapping = call_with_self_heal(messages, llm, AssetMapping)
    logger.info(
        f"Asset mapping response:\n```json\n{asset_mapping.model_dump_json(indent=2)}\n```"
    )
    predicate = asset_mapping.predicate
    patch = asset_mapping.patch

    invalid_predicate = ValueError(
        f"Mapping function must include all predicate fields: {asset_spec.predicate_fields}, got: {predicate}"
    )

    if operation == "update":
        if len(predicate) != len(asset_spec.predicate_fields):
            raise invalid_predicate
        if len(patch) == 0:
            raise ValueError("Mapping function must include at least one patch field")
    elif operation == "delete":
        if len(predicate) != len(asset_spec.predicate_fields):
            raise invalid_predicate
    elif operation == "create":
        raise ValueError("Create operation is not supported yet")

    input_fields = example.keys()
    patch_input_fields = [x[0] for x in patch]
    output_fields = asset_spec.fields.values()
    predicate_output_fields = [x[1] for x in predicate]
    patch_output_fields = [x[1] for x in patch]

    for f in predicate_output_fields:
        if f not in output_fields:
            raise ValueError(f"Predicate output field '{f}' not found in spec fields")
    for f in patch_output_fields:
        if f not in output_fields:
            raise ValueError(f"Patch output field '{f}' not found in spec fields")
    for f in patch_input_fields:
        if f not in input_fields:
            raise ValueError(
                f"Patch input field '{f}' not found in example input fields"
            )

    return asset_mapping


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
   - For fields like "types" or "categories", each value must be an object of the form:  
     { "id": "<value>" }
   - If the input already has {"id": "...", "code": "..."}, leave it unchanged.

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

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(
            content="Please convert the following patch:\n"
            + json.dumps(patches, indent=2)
        ),
    ]

    class Response(BaseModel):
        results: List[Dict[str, Any]]

    response = call_with_self_heal(messages, llm, Response)
    return response.results


def map_data(
    llm: ChatOpenAI, data_migration: DataMigration, op_index: int
) -> List[AssetPatch]:
    op = data_migration.operations[op_index]
    asset_spec = op["asset_spec"]
    if asset_spec is None:
        raise ValueError("Asset spec is required for the operation")
    input_data = op.data or []
    if len(input_data) == 0:
        raise ValueError("No input data found for the operation")
    example_input = input_data[0]
    logger.info(f"Creating asset mapping for {op['asset_type']} {op['operation']}")
    asset_mapping = _create_asset_mapping(
        llm, op["operation"], asset_spec, example_input
    )
    logger.info(f"Mapping data started for {op['asset_type']} {op['operation']}")
    mapped_data_with_specs = _run_mapping_with_specs(input_data, asset_mapping)
    logger.info(f"Mapping data completed for {op['asset_type']} {op['operation']}")

    specs_form_patches = [
        {k: v for k, v in x.patch.items() if "name" in v}
        for x in mapped_data_with_specs
    ]

    if any(len(x) > 0 for x in specs_form_patches):
        logger.info("Patch conversion started")
        results = _resolve_patch_specs(llm, specs_form_patches)
        logger.info("Patch conversion completed")
        if len(results) != len(mapped_data_with_specs):
            raise ValueError(
                f"Expected {len(mapped_data_with_specs)} results, got {len(results)}"
            )
        for i, result in enumerate(results):
            if set(result.keys()) != set(specs_form_patches[i].keys()):
                raise ValueError(
                    f"Expected keys {set(specs_form_patches[i].keys())}, got {set(result.keys())}"
                )
            patch = mapped_data_with_specs[i].patch
            mapped_data_with_specs[i].patch = {**patch, **result}

    logger.info("Unflattening mapped data")
    for x in mapped_data_with_specs:
        x.patch = unflatten(x.patch)
        for key in x.patch:
            lib_entry_key, asset_type = next(
                (y for y in asset_spec.enrichable_fields if y[0] == key),
                (None, None),
            )
            if lib_entry_key is not None and "code" not in x.patch[key]:
                x.patch[key][
                    "code"
                ] = f"must be enriched from mongodb collection cached_{asset_type}"
    logger.info("Unflattening completed")

    return mapped_data_with_specs
