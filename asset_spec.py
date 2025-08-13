from typing import Dict
from app_types import AssetSpec, AssetType


ASSET_SPECS: Dict[AssetType, AssetSpec] = {
    "BaseMaterial": AssetSpec(
        fields={
            "Vendor Code": "organizationId",
            "Base Material Vendor Code": "vendorCode",
            "Base Material Vendor Description": "vendorDescription",
            "Material Family KEYE Code": "materialFamily.id",
            "Material Family KEYE Description": "materialFamily.code",
            "Material Family Vendor Code": "vendorMaterialFamily.id",
            "Material Family Vendor Description": "vendorMaterialFamily.code",
            "Base Material KEYE Code": "material.id",
            "Base Material KEYE Description": "material.code",
        },
        predicate_fields=["organizationId", "vendorCode"],
        enrichable_fields=[("material", "BaseMaterialLibraryEntry")],
    ),
    "SupplierLibraryEntry": AssetSpec(
        fields={
            "Supplier VAT number / Registration Number": "key",
            "SAP Supplier Code": "sapCode",
            "Supplier Country Code": "country.id",
            "Supplier Country Description": "country.code",
            "Supplier Name": "description",
            "Semi Finished Supplier": {
                "name": "semiFinishedSupplier",
                "type": "boolean",
            },
            "Supplier Type": {"name": "types", "type": "array", "array_value_type": "LibraryEntry"},
            "Supplier Status": {"name": "disabled", "type": "boolean"},
            "Catalog Uploaded By": "catalogUploadedBy",
            "Visibility Rules": {"name": "hasVisibilityRules", "type": "boolean"},
        },
        predicate_fields=["key"],
    ),
}
