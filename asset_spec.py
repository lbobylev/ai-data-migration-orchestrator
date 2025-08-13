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
            "Supplier Type": {
                "name": "types",
                "type": "array",
                "array_value_type": "LibraryEntry",
            },
            "Supplier Status": {"name": "disabled", "type": "boolean"},
            "Catalog Uploaded By": {
                "name": "catalogUploadedBy",
                "type": "string",
                "nullable": True,
            },
            "Visibility Rules": {"name": "hasVisibilityRules", "type": "boolean"},
        },
        predicate_fields=["key"],
        create_required_fields=[
            "key",
            "country.id",
            "description",
            "semiFinishedSupplier",
            "hasVisibilityRules",
            "disabled",
            "types",
        ],
    ),
    "Organization": AssetSpec(
        fields={
            "Supplier VAT number / Registration Number": "attributes.vatCode",
            "SAP Supplier Code": "attributes.sapCode",
            "Supplier Name": "companyName",
            "Supplier Type": {
                "name": "companyTypes",
                "type": "array",
                "array_value_type": "string",
            },
        },
        predicate_fields=["attributes.vatCode"],
        create_required_fields=["companyName", "attributes.vatCode", "companyTypes"],
    ),
    "EyewearManufacturerAssignment": AssetSpec(
        fields={
            "UPC Code": "eyewearId",
            "Frame Manufacturer VAT Number / Registration Code": {
                "name": "manufacturerId",
                "type": "string",
                "relation": {
                    "asset_type": "Organization",
                    "target_field": "companyId",
                    "predicate_field": "attributes.vatCode",
                },
            },
        },
        predicate_fields=["eyewearId", "manufacturerId"],
    ),
}
