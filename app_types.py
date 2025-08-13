from typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Tuple,
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

Operation = Literal["create", "update", "delete"]

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
    "other",
]

T = TypeVar("T", bound=Dict[str, Any])
P = TypeVar("P", bound=Dict[str, Any])


class LibraryEntry(BaseModel):
    id: str = Field(description="The unique identifier for the library entry.")
    code: str = Field(description="Desciription or code of the library entry.")


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


class AssetSpec(BaseModel):
    fields: Dict[str, str | AssetFieldSpec] = Field(
        description="A mapping of field names to their corresponding keys in the asset."
    )
    predicate_fields: List[str] = Field(
        description="A list of fields that are used as predicates for the asset."
    )
    enrichable_fields: List[Tuple[str, LibraryEntryType]] = Field(
        default_factory=list,
        description="A list of fields that can be enriched with additional data.",
    )


class AssetOperation(BaseModel):
    asset_type: AssetType = Field(description="The type of the asset.")
    operation: Operation = Field(
        description="The operation to be performed on the asset."
    )
    asset_spec: AssetSpec = Field(
        default=AssetSpec(fields={}, predicate_fields=[]),
        description="The specification of the asset, including fields and predicates.",
    )
    data: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="The data associated with the asset operation.",
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
    environment: list[Environment] = Field(
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


class MyState(TypedDict, total=False):
    issue: GithubIssue
    user_prompt: str
    user_input: str
    status: Status
    task: Task | None
    detected_operations: List[AssetOperation] | None
    op: Annotated[AssetOperation | None, take_last]
    op_index: Annotated[int | None, take_last]
    results: Annotated[List[Dict[str, Any]] | None, add]
    errors: Annotated[List[Dict[str, Any]] | None, add]
    total: int | None
    done: Annotated[int | None, add]


class DockerResponse(BaseModel):
    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool
    duration_sec: float


ASSET_OPERATIONS: Dict[Operation, str] = {
    "create": "Create a new asset.",
    "update": "Update an existing asset.",
    "delete": "Delete an existing asset.",
}

ASSET_TYPES = list(get_args(AssetType))

ENVIRONMENTS = list(get_args(Environment))
