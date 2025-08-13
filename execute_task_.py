from app_types import AssetPatch
from bc.kube_utils import PortForwardHandle, start_port_forwarding, stop_port_forwarding
from bc.run_tasks import run_tasks
from logger import get_logger
from operation_helpers import ExecutionTask

logger = get_logger(__name__)

json = """
{
    "predicate": {
        "key": "IT04092700121"
    },
    "patch": {
        "sapCode": "107681",
        "country": {
            "id": "IT",
            "code": "Italy"
        },
        "description": "MIRAGE SRL",
        "semiFinishedSupplier": false,
        "types": [
            {
                "id": "Frame Manufacturer",
                "code": "Frame Manufacturer"
            }
        ],
        "disabled": true,
        "catalogUploadedBy": null,
        "hasVisibilityRules": false,
        "key": "IT04092700121",
        "id": "common.supplier.it04092700121",
        "namespace": "common",
        "library": "supplier",
        "rank": null,
        "extra": null,
        "createdBy": "Surge Agent",
        "createdAt": "2025-08-29T11:59:51.624759",
        "visibleTo": [],
        "organizationId": "mirage"
    }
}
"""

env = "prod"


def create():
    patch = AssetPatch.model_validate_json(json)
    task = ExecutionTask(
        asset_type="SupplierLibraryEntry", operation="create", patches=[patch]
    )
    return task


def delete():
    patch = AssetPatch(predicate={"id": "common.supplier.it04092700121"}, patch={})
    return ExecutionTask(
        asset_type="SupplierLibraryEntry", operation="delete", patches=[patch]
    )


if __name__ == "__main__":
    #task = create()
    task = delete()
    handle: PortForwardHandle
    try:
        handle = start_port_forwarding(env)
        run_tasks(environment=env, tasks=[task], dry_run=True)
    except Exception as e:
        logger.error(f"Error executing tasks in environment {env}: {e}")
    finally:
        stop_port_forwarding(handle)
