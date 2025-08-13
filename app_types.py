from __future__ import annotations
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    TypeVar,
    TypedDict,
    get_args,
)

from pydantic import BaseModel, Field

from typing_extensions import Annotated
from operator import add


def take_last(_, b):
    # in case of conflict, take the last value (branches are independent, order does not matter)
    return b


DataSource = Literal["attachment_file", "user_request", "other"]

Environment = Literal["prod", "preprod", "test", "dev"]

Operation = Literal["create", "update", "delete", "deprecation"]

BugType = Literal["export_issue", "other"]

LibraryEntryType = Literal[
    "BaseMaterialLibraryEntry",
    "CatalogObjectTypeLibraryEntry",
    "ComponentTypeLibraryEntry",
    "LibraryEntry",
    "RegionLibraryEntry",
    "SupplierLibraryEntry",
    "UnitOfMeasureLibrary",
]


AssetType = Literal[
    "Acetate",
    "AcetateCertificate",
    "AcetateDataSheet",
    "AcetateManufacturerCertificate",
    "AcetateProposal",
    "Attachment",
    "Audit",
    "BaseMaterial",
    "BaseMaterialLibraryEntry",
    "Bom",
    "CaseItem",
    "CaseItemCertificate",
    "CaseItemTechInfo",
    "CaseItemVersion",
    "CaseManufacturerCertificate",
    "CaseSet",
    "CaseSetSupplierAssignment",
    "CatalogObjectTypeLibrary",
    "CatalogObjectTypeLibraryEntry",
    "CertificationRequest",
    "ComponentCertificate",
    "ComponentManufacturerCertificate",
    "ComponentReference",
    "ComponentSuggestedMeasure",
    "ComponentTypeLibrary",
    "ComponentTypeLibraryEntry",
    "Content",
    "Counter",
    "CustomComponent",
    "DamActivity",
    "DamBackUpData",
    "DamSeasonDate",
    "DamUser",
    "DamUserGroup",
    "DataChangeMeta",
    "DataChangeRequest",
    "DocumentInfo",
    "Eyewear",
    "EyewearAcetateLink",
    "EyewearComponentLink",
    "EyewearDesignerCertificate",
    "EyewearDropBallTest",
    "EyewearGalvanicTreatmentLink",
    "EyewearManufacturerAssignment",
    "EyewearManufacturerCertificate",
    "EyewearMediaImage",
    "EyewearMediaImagePreviewRanking",
    "EyewearTechInfo",
    "EyewearWithCaseSetLink",
    "EyewearWithComponentReferenceLink",
    "GalvanicTreatment",
    "GalvanicTreatmentCertificate",
    "GalvanicTreatmentDataSheet",
    "GalvanicTreatmentManufacturerCertificate",
    "GenericContent",
    "GenericContentCategory",
    "Hinge",
    "Lens",
    "LensDataSheet",
    "LensDataSheetRevision",
    "LensDropBallTest",
    "LensManufacturerCertificate",
    "Library",
    "LibraryEntry",
    "MainPartLibrary",
    "ManualComponent",
    "ManualGalvanicTreatment",
    "MaterialReference",
    "MigrationStatus",
    "MissingComponentRequest",
    "MissingGalvanicTreatmentRequest",
    "NosePad",
    "OptiTest",
    "Organization",
    "PackagingItem",
    "PackagingItemTechInfo",
    "PackagingItemVersion",
    "PackagingSet",
    "PackagingSetSupplierAssignment",
    "PackagingSetTechInfo",
    "PackagingSetVersion",
    "Pads",
    "PlatingMaterial",
    "PlatingMaterialCertificate",
    "PreviewImageContent",
    "Product",
    "ProductComponent",
    "ProductComponentCertificate",
    "ProductComponentSupplierAssignment",
    "ProductComponentTechInfo",
    "ProductComponentVersion",
    "ProductStatusUpdate",
    "RegionLibraryEntry",
    "Role",
    "Screw",
    "SupplierLibraryEntry",
    "TechDesign",
    "UnitOfMeasureLibrary",
    "User",
    "VmPopSnapshot",
    "Wirecore",
]

TaskType = Literal["data_migration", "bug", "other"]

Status = Literal[
    "schema_validation_passed",
    "file_selected",
    "file_selection_failed",
    "file_loaded",
    "file_load_failed",
    "data_extracted",
    "data_extraction_failed",
    "task_classification_failed",
    "data_migration_detected",
    "data_migration_classified",
    "data_migration_classification_failed",
    "bug_detected",
    "bug_classification_failed",
    "export_issue_detected",
    "data_split_failed",
    "operations_detected",
    "operations_detection_failed",
    "data_distribution_failed",
    "data_distributed",
    "other",
]


T = TypeVar("T", bound=Dict[str, Any])
P = TypeVar("P", bound=Dict[str, Any])


class LibraryEntry(BaseModel):
    id: str = Field(description="The unique identifier for the library entry.")
    code: Optional[str] = Field(
        description="Desciription or code of the library entry.", default=None
    )


class AssetPatch(BaseModel, Generic[P, T]):
    predicate: P = Field(description="The predicate for the base material update.")
    patch: T = Field(description="The patch for the base material update.")


AssetFieldType = (
    Literal["string", "number", "boolean", "LibraryEntry", "array"] | AssetType
)


class AssetFieldSpec(TypedDict, total=False):
    name: str
    type: AssetFieldType
    array_value_type: AssetFieldType
    nullable: bool


class AssetSpec(BaseModel):
    fields: Dict[str, str | AssetFieldSpec] = Field(
        description="A mapping of field names to their corresponding keys in the asset."
    )
    predicate_fields: List[str] = Field(
        description="A list of fields that are used as predicates for the asset."
    )


class AssetOperation(BaseModel):
    asset_type: AssetType = Field(description="The type of the asset.")
    operation_name: Operation = Field(
        description="The operation to be performed on the asset."
    )
    data: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="The data associated with the asset operation.",
    )
    patches: Dict[Environment, List[AssetPatch]] = Field(
        default_factory=dict,
        description="A mapping of environments to their corresponding list of patches.",
    )

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            raise KeyError(f"Key '{key}' does not exist in AssetOperation.")

    def __getitem__(self, item):
        return getattr(self, item)


class Task(BaseModel):
    task_type: TaskType = Field(
        description="The type of the task, which can be data migration, bug, or other."
    )


class Bug(Task):
    task_type: TaskType = Field(default="bug")
    bug_type: BugType = Field(default="other")
    orgs: list[str] = Field(
        default_factory=list,
        description="The list of organizations affected by the bug.",
    )
    environment: list[Environment] = Field(
        default_factory=list,
        description="The environment(s) where the task is applicable.",
    )


class DataMigration(Task):
    task_type: TaskType = Field(default="data_migration")
    operations: list[AssetOperation] = Field(
        default_factory=list,
        description="The list of asset operations to be performed.",
    )
    environments: list[Environment] = Field(
        default_factory=list,
        description="The environment(s) where the task is applicable.",
    )
    data_source: DataSource = Field(
        default="other",
        description="The source of the data for the operation.",
    )
    file_url: str | None = Field(
        default=None,
        description="The URL of the file if the operation is related to a file.",
    )
    body: str | None = Field(
        default=None,
        description="The body of the user request if the data source is user_request.",
    )
    data: List[Dict[str, Any]] | None = Field(
        default=None,
        description="The data extracted from the file or user request.",
    )


class GithubIssue(TypedDict):
    number: int
    title: str
    body: str


class OperationResult(TypedDict):
    index: int
    operations: List[AssetOperation]


class OperationError(TypedDict):
    op: Optional[AssetOperation]
    error: str


class MyState(TypedDict, total=False):
    issue: GithubIssue
    user_prompt: str
    user_input: str
    status: Status
    task: Annotated[Task | None, take_last]
    detected_asset_types: List[AssetType] | None
    detected_operations: List[AssetOperation]
    op: Annotated[AssetOperation, take_last]  # current operation being processed
    op_index: Annotated[int, take_last]  # index of current operation being processed
    operation_results: Annotated[List[OperationResult] | None, add]
    operation_errors: Annotated[List[OperationError] | None, add]
    operation_total: Annotated[int, add]
    operation_done: Annotated[int, add]


def state_operations_error(error: str, op: Optional[AssetOperation] = None) -> MyState:
    return {"operation_errors": [{"op": op, "error": error}], "operation_done": 1}


def state_operations_result(result: OperationResult) -> MyState:
    return {"operation_results": [result], "operation_done": 1}


class DockerResponse(BaseModel):
    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool
    duration_sec: float


ASSET_OPERATIONS: Dict[Operation, str] = {
    "create": """
Create new assets and add them to a library, catalog, or collection. This represents actions such as "add to library," "add to catalog," or "create in."
""",
    "update": "Update an existing asset.",
    "delete": "Delete an existing asset.",
    "deprecation": "Deprecate an existing asset.",
}

ASSET_TYPES = list(get_args(AssetType))

ENVIRONMENTS = list(get_args(Environment))
