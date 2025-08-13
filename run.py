import json
from typing import List

from langchain_openai import ChatOpenAI
from logger import get_logger
from operation_helpers import _resolve_patch_specs

logger = get_logger(__name__)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

specs_json = """
[
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
      },
      {
        "semiFinishedSupplier": {
          "name": "semiFinishedSupplier",
          "type": "boolean",
          "value": "No"
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
      },
      {
        "semiFinishedSupplier": {
          "name": "semiFinishedSupplier",
          "type": "boolean",
          "value": "No"
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
      },
      {
        "semiFinishedSupplier": {
          "name": "semiFinishedSupplier",
          "type": "boolean",
          "value": "No"
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
      },
      {
        "semiFinishedSupplier": {
          "name": "semiFinishedSupplier",
          "type": "boolean",
          "value": "No"
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
      },
      {
        "semiFinishedSupplier": {
          "name": "semiFinishedSupplier",
          "type": "boolean",
          "value": "No"
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
      },
      {
        "semiFinishedSupplier": {
          "name": "semiFinishedSupplier",
          "type": "boolean",
          "value": "No"
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
      },
      {
        "semiFinishedSupplier": {
          "name": "semiFinishedSupplier",
          "type": "boolean",
          "value": "No"
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
      },
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
      },
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
      },
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
      },
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
      },
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
      },
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
      },
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
    ]
"""

if __name__ == "__main__":
    specs: List[dict] = json.loads(specs_json)
    print(len(specs))
    for n in range(1):
        print(f"--- Iteration {n+1} ---")
        try:
            result = _resolve_patch_specs(llm, specs)
            print(len(result))
        except Exception as e:
            logger.error(f"Error resolving patch specs: {e}")
