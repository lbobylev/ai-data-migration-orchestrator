from typing import Any, Dict, List, Set

from app_types import AssetType, Environment
from bc.cache_utils import reload_cache
from bc.chaincode_api import BlockchainApi
from logger import get_logger
from operation_helpers import ExecutionTask

logger = get_logger(__name__)


def _create_predicate_fn(pred: Dict[str, Any]):
    def _matches(asset: Dict[str, Any]) -> bool:
        return all(asset.get(k) == v for k, v in pred.items())

    return _matches


def run_tasks(
    *,
    environment: Environment,
    tasks: List[ExecutionTask],
    dry_run: bool = True,
) -> None:
    """
    Apply create/update/delete asset operations against the blockchain API.

    Uses logger for structured logging instead of print.
    """

    api = BlockchainApi("localhost", 3000, dry_run)

    if not tasks:
        logger.info("No asset operations to perform.")
        return

    logger.info("Dry run mode: %s", dry_run)
    logger.info("Blockchain url: %s", api.base_url)

    cache_types: Set[AssetType] = set()

    for task in tasks:
        asset_type = task.asset_type
        cache_types.add(asset_type)
        logger.info(
            "Processing operation=%s for asset type=%s", task.operation, asset_type
        )

        if task.operation == "create":
            batch_creates = [p.patch for p in task.patches]
            for new_asset in batch_creates:
                logger.debug(
                    "Creating new asset of type %s with data: %s", asset_type, new_asset
                )

            logger.info(
                "Saving batch create for %d assets of type %s",
                len(batch_creates),
                asset_type,
            )
            if batch_creates:
                api.save_batch(asset_type, batch_creates)
            logger.info(
                "Batch create saved for %d assets of type %s",
                len(batch_creates),
                asset_type,
            )

        elif task.operation == "delete":
            assets = api.find_all(asset_type)
            logger.info("Found %d assets of type %s", len(assets), asset_type)

            try:
                from bc.chaincode_api import id_mapper

                id_key = id_mapper(asset_type)
            except Exception:
                id_key = "id"

            to_delete_ids: List[str] = []
            for p in task.patches:
                matches = _create_predicate_fn(p.predicate)
                match = next((a for a in assets if matches(a)), None)
                if not match:
                    logger.warning(
                        "No matching asset found for type=%s predicate=%s",
                        asset_type,
                        p.predicate,
                    )
                    continue
                aid = match.get(id_key)
                logger.info("Deleting asset of type %s with ID %s", asset_type, aid)
                to_delete_ids.append(str(aid))

            logger.info(
                "Deleting batch of %d assets of type %s", len(to_delete_ids), asset_type
            )
            if to_delete_ids:
                api.delete_batch(asset_type, to_delete_ids)
            logger.info(
                "Deleted batch of %d assets of type %s", len(to_delete_ids), asset_type
            )

        elif task.operation == "update":
            assets = api.find_all(asset_type)
            logger.info("Found %d assets of type %s", len(assets), asset_type)

            batch_updates: List[Dict[str, Any]] = []
            for p in task.patches:
                matches = _create_predicate_fn(p.predicate)
                match = next((a for a in assets if matches(a)), None)
                if not match:
                    logger.warning(
                        "No matching asset found for type=%s predicate=%s",
                        asset_type,
                        p.predicate,
                    )
                    continue
                logger.debug(
                    "Applying patch to asset type=%s before=%s", asset_type, match
                )
                merged = {**match, **p.patch}
                batch_updates.append(merged)
                logger.debug("Patched asset of type %s after=%s", asset_type, merged)

            logger.info(
                "Saving batch update for %d assets of type %s",
                len(batch_updates),
                asset_type,
            )
            if batch_updates:
                api.save_batch(asset_type, batch_updates)
            logger.info(
                "Batch update saved for %d assets of type %s",
                len(batch_updates),
                asset_type,
            )

        else:
            raise ValueError(f"Unsupported operation: {task.operation}")

    # --- Refresh cache for affected asset types ---

    logger.info(f"Refreshing cache for asset types={", ".join(cache_types)}")
    if not dry_run:
        try:
            reload_cache(environment, None, list(cache_types), None)
        except Exception as e:
            logger.warning(
                "Failed to refresh cache for asset types=%s: %s",
                ", ".join(cache_types),
                e,
            )
    logger.info("Cache refresh completed")

    logger.info("Asset operations completed successfully")
