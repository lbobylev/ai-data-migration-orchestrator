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

TaskType = Literal[
    "data_migration",
    "bug",
    "delete_notifications",
    "delete_organization_by_id",
    "other",
]

Status = Literal[
    "environment_detection_failed",
    "environment_detected",
    "user_input_processing_failed",
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
    "operation_detected",
    "operation_detected_for_patch_extraction",
    "operation_detection_failed",
    "data_distribution_failed",
    "data_distributed",
    "delete_notifications_detected",
    "delete_notifications_failed",
    "asset_type_detected",
    "asset_type_detection_failed",
    "data_source_detected",
    "data_source_detection_failed",
    "delete_organization_by_id_detected",
    "tasks_created",
    "task_creation_failed",
    "no_task_to_execute",
    "tasks_executed",
    "task_execution_failed",
    "no_tabular_data_found",
    "patches_extracted",
    "patch_extraction_failed",
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


class AssetRelation(TypedDict, total=False):
    """
    Defines a relationship between assets.
    """

    asset_type: AssetType  # The type of the related asset.
    target_field: str  # The field in the related asset that this relation points to.
    predicate_field: (
        str  # The field in the input data that is used to find the related asset.
    )
    predicate_field_value: Optional[
        str
    ]  # An optional value to match in the predicate field.


class AssetFieldSpec(TypedDict, total=False):
    name: str
    type: AssetFieldType
    array_value_type: AssetFieldType
    nullable: bool
    relation: Optional[AssetRelation]


class AssetSpec(BaseModel):
    fields: Dict[str, str | AssetFieldSpec] = Field(
        description="A mapping of field names to their corresponding keys in the asset."
    )
    predicate_fields: List[str | AssetFieldSpec] = Field(
        description="A list of fields that are used as predicates for the asset."
    )
    create_required_fields: List[str] = Field(
        default_factory=list,
        description="A list of fields that are required when creating a new asset.",
    )


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


class GithubIssue(TypedDict):
    number: int
    title: str
    body: str


class MyState(TypedDict, total=False):
    user_prompt: str
    user_input: str
    status: Status
    task_type: TaskType
    asset_type: Optional[AssetType]
    environments: Optional[List[Environment]]
    data_source: Optional[DataSource]
    file_url: Optional[str]
    detected_operation: Operation
    data: Optional[List[Dict[str, Any]]]
    tasks: Optional[Dict[Environment, List[ExecutionTask]]]
    dry_run: Optional[bool]
    patches: Optional[List[AssetPatch]]


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
    "delete": "Delete or reset an existing asset.",
    "deprecation": "Deprecate an existing asset.",
}

ASSET_TYPES = list(get_args(AssetType))

ENVIRONMENTS = list(get_args(Environment))


class ExecutionTask(BaseModel):
    asset_type: AssetType
    operation: Operation
    patches: List[AssetPatch]
