from typing import Callable, Dict, Tuple, Optional
from app_types import AssetType, Operation, AssetPatch
from datetime import datetime
from db import mongo
from logger import get_logger
import re

logger = get_logger(__name__)

countries = {
    "JP": "Japan",
    "CN": "China",
    "HK": "Hong Kong",
    "MU": "Mauritius",
    "US": "United States",
    "MO": "Macao",
    "TW": "Taiwan, Province of China",
    "AT": "Austria",
    "CH": "Switzerland",
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "SI": "Slovenia",
    "SK": "Slovakia",
}


# Xiamen Torch Special Metal Material Co., LTD
# xiamen-torch-special-metal-material-co-ltd
def company_name_to_id(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "-", name)
    name = re.sub(r"-+", "-", name)
    name = name.strip("-")
    return name


def supplier_type_to_id(supplier_type: str) -> Optional[str]:
    types = {
        "Certification Authority": "ceraut",
        "Component/Raw Material Supplier": "cmpman",
        "Eyewear Designer": "eyedes",
        "Frame Manufacturer": "eyeman",
        "Galvanic Treatment Supplier": "galvman",
        "Packaging Supplier": None,
    }

    return types.get(supplier_type)


def _enrich_sulplier_library_entry(patch: Dict):
    key = patch["key"]
    namespace = "common"
    library = "supplier"
    if "id" not in patch:
        patch["id"] = f"{namespace}.{library}.{key.lower()}"
    if "namespace" not in patch:
        patch["namespace"] = namespace
    if "library" not in patch:
        patch["library"] = library
    if "rank" not in patch:
        patch["rank"] = None
    if "extra" not in patch:
        patch["extra"] = None
    if "createdBy" not in patch:
        patch["createdBy"] = "Surge Agent"
    if "createdAt" not in patch:
        patch["createdAt"] = datetime.now().isoformat()
    if "sapCode" not in patch:
        patch["sapCode"] = None
    if "visibleTo" not in patch:
        patch["visibleTo"] = []
    if "types" in patch and isinstance(patch["types"], list):
        for type in patch["types"]:
            if "id" in type and "code" not in type:
                type["code"] = type["id"]
    if "country" in patch and isinstance(patch["country"], dict):
        country = patch["country"]
        if "id" in country and "code" not in country:
            country["code"] = countries.get(country["id"], None)
    if "sapCode" in patch and patch["sapCode"] == "":
        patch["sapCode"] = None


def find_organization_id_by_vat(vat_code: str) -> str:
    org = (
        mongo()
        .db("kering")
        .collection("cached_Organization")
        .find_one({"attributes.vatCode": vat_code})
    )
    if org is None:
        raise ValueError(f"Organization with vatCode={vat_code} not found")
    else:
        logger.info(f"Found organization {org['companyId']} for vatCode={vat_code}")
    return org["companyId"]


def encrich_supplier_library_entry_deprecation(
    asset_patch: AssetPatch, prev_vat_code: str
):
    patch = asset_patch.patch
    _enrich_sulplier_library_entry(patch)
    if "organizationId" not in patch:
        patch["organizationId"] = find_organization_id_by_vat(prev_vat_code)


def encrich_supplier_library_entry_create(asset_patch: AssetPatch):
    patch = asset_patch.patch
    _enrich_sulplier_library_entry(patch)
    if "organizationId" not in patch:
        company_name = patch.get("description", "")
        patch["organizationId"] = company_name_to_id(company_name)


def organization_create(asset_patch: AssetPatch):
    patch = asset_patch.patch
    id = company_name_to_id(patch["companyName"])
    patch["companyId"] = id
    patch["id"] = id
    company_types = []
    for t in patch.get("companyTypes", []):
        type_id = supplier_type_to_id(t)
        if type_id is None:
            logger.warning(f"Unknown supplier type: {t}")
            raise ValueError(f"Unknown supplier type: {t}")
        company_types.append(type_id)
    patch["companyTypes"] = company_types
    if "active" not in patch:
        patch["active"] = False
    if "attributes" in patch and isinstance(patch["attributes"], dict):
        attributes = patch["attributes"]
        if "sapCode" in attributes and attributes["sapCode"] == "":
            attributes["sapCode"] = None

def enrich_eyewear_manufacturer_assignment_delete(asset_patch: AssetPatch):
    predicate = asset_patch.predicate
    if "manufacturerId" in predicate:
        manufacturerId = predicate["manufacturerId"]
        if isinstance(manufacturerId, dict) and "relation" in manufacturerId:
            vatCode = manufacturerId["relation"].get("predicate_field_value", None)
            if vatCode is not None:
                predicate["manufacturerId"] = find_organization_id_by_vat(vatCode)


ENRICHERS: Dict[Tuple[AssetType, Operation], Callable] = {
    ("SupplierLibraryEntry", "deprecation"): encrich_supplier_library_entry_deprecation,
    ("SupplierLibraryEntry", "create"): encrich_supplier_library_entry_create,
    ("Organization", "create"): organization_create,
    ("EyewearManufacturerAssignment", "delete"): enrich_eyewear_manufacturer_assignment_delete,
}
