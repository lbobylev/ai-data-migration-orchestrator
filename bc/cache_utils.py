import os
import requests
import yaml
from pathlib import Path
from typing import List, Optional, Dict, Protocol
from pydantic import BaseModel

from app_types import AssetType, Environment
from http_utils import retry_call
from logger import get_logger


# Optional: load .env if python-dotenv is present; otherwise noop.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

logger = get_logger(__name__)


class ReloadCacheParams(BaseModel):
    env: Environment
    org: Optional[List[str]] = None
    include: Optional[List[AssetType]] = None
    exclude: Optional[List[AssetType]] = None


def _resolve_secrets_path() -> Path:
    """
    Determine the secrets.yaml path from environment.

    Priority:
    1) SECRETS_PATH in environment (e.g., from .env)
    2) default to 'secrets.yaml' in CWD
    """
    # Accept both SECRETS_PATH and ULTRA_CACHE_SECRETS for flexibility
    env_path = os.getenv("SECRETS_PATH") or os.getenv("ULTRA_CACHE_SECRETS")
    return Path(env_path) if env_path else Path("secrets.yaml")


def _load_secrets(path: Optional[Path] = None) -> Dict[str, Dict[str, str]]:
    path = path or _resolve_secrets_path()
    try:
        with open(path, "r") as f:
            secrets = yaml.safe_load(f)
        return secrets or {}
    except FileNotFoundError:
        raise RuntimeError(f"secrets file not found at: {path}")
    except yaml.YAMLError as e:
        raise RuntimeError(f"Failed to load {path}: {e}")


def _get_all_orgs(secrets: Dict[str, Dict[str, str]]) -> List[str]:
    return list(secrets.keys())


def _validate_orgs(
    params: ReloadCacheParams, secrets: Dict[str, Dict[str, str]]
) -> None:
    if params.org:
        invalid = [o for o in params.org if o not in secrets]
        if invalid:
            raise ValueError(
                f"Invalid organizations {invalid}, must be among {_get_all_orgs(secrets)}"
            )

        # check env for first org
        if params.env not in secrets[params.org[0]]:
            raise ValueError(
                f"Environment {params.env} not found for organization {params.org[0]} in secrets"
            )


def _reload_cache(
    params: ReloadCacheParams, secrets: Dict[str, Dict[str, str]]
) -> None:
    orgs = params.org if params.org else _get_all_orgs(secrets)
    data: Dict[str, Dict[str, List[str]]] = {"default": {}}
    if params.include:
        data["default"]["include"] = [str(x) for x in params.include]
    if params.exclude:
        data["default"]["exclude"] = [str(x) for x in params.exclude]

    logger.info(
        "Starting ultra-cache reload on env=%s; orgs=%s; include=%s; exclude=%s",
        params.env,
        orgs,
        params.include,
        params.exclude,
    )

    for o in orgs:
        host = (
            f"https://{o}{'' if params.env == 'prod' else '.' + params.env}.cp-bc.com"
        )
        url = f"{host}/api/v1.0/ultra-cache/data/refresh"

        try:
            response = retry_call(
                lambda: requests.post(
                    url,
                    headers={"Surge-Machine-Secret": secrets[o][params.env]},
                    json=data,
                    timeout=120,
                )
            )
            if response.status_code == 200:
                logger.info("Ultra-cache reload OK for org=%s (%s)", o, host)
            else:
                logger.error(
                    "Ultra-cache reload FAILED for org=%s (%s): status=%s body=%s",
                    o,
                    host,
                    response.status_code,
                    response.text[:500],
                )
        except Exception as e:
            logger.exception(
                "Error reloading ultra-cache for org=%s (%s): %s", o, host, e
            )

    logger.info("Ultra-cache reload finished")


def reload_cache(
    env: Environment,
    organizations: Optional[List[str]] = None,
    include: Optional[List[AssetType]] = None,
    exclude: Optional[List[AssetType]] = None,
    secrets_path: Optional[Path] = None,  # path now comes from .env by default
) -> None:
    """
    Main entrypoint function for reloading ultra-cache.

    :param env: Environment (dev, exp, preprod, prod, test)
    :param org: One or more organization names (optional, defaults to all)
    :param include: Assets to include (optional)
    :param exclude: Assets to exclude (optional)
    :param secrets_path: Optional explicit path to secrets.yaml; if None, read from .env (SECRETS_PATH)
    """
    params = ReloadCacheParams(
        env=env, org=organizations, include=include, exclude=exclude
    )
    secrets = _load_secrets(secrets_path)
    _validate_orgs(params, secrets)
    _reload_cache(params, secrets)
    logger.info("Done")
