import subprocess
import atexit
from typing import Any, Callable, Dict, List, Optional, get_args
from pydantic import BaseModel
from pymongo import MongoClient

from app_types import AssetType, Environment
from bc.kube_utils import switch_context
from logger import get_logger

MONGO_URI = "mongodb://localhost:27017"
APP_NAME = "surge-agent"
TIMEOUT_MS = 5000

_port_forward_process = None
_client = None

logger = get_logger(__name__)


def start_port_forward(env: Environment, namespace: Optional[str] = None):
    """
    Launches kubectl port-forward in the background if it is not already running.
    """
    global _port_forward_process

    if isinstance(_port_forward_process, subprocess.Popen):
        logger.warning("Port-forward already running.")
        return

    switch_context(env)

    if namespace is None:
        namespace = "shared" if env == "dev" else "kering"

    logger.info(
        f"🚀 Starting mongo port-forward for env: {env} in namespace: {namespace}..."
    )
    cmd = [
        "kubectl",
        "-n",
        f"{namespace}-{env}",
        "port-forward",
        f"{namespace}-mongo-mongodb-0",
        "27017:27017",
    ]
    logger.debug(f"Port-forward command: {' '.join(cmd)}")
    # Run it in the background, redirect stdout and stderr so it doesn't block
    _port_forward_process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Register the process termination on exit
    atexit.register(stop_port_forward)


def stop_port_forward():
    """
    Stops the kubectl port-forward.
    """
    global _port_forward_process
    if (
        isinstance(_port_forward_process, subprocess.Popen)
        and _port_forward_process.poll() is None
    ):
        logger.info("🛑 Stop mongo port-forward...")
        _port_forward_process.terminate()
        try:
            _port_forward_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _port_forward_process.kill()
        _port_forward_process = None


def get_client() -> MongoClient:
    """
    Возвращает MongoClient, при первом вызове поднимает port-forward.
    """
    global _client

    if _client is None:
        _client = MongoClient(
            MONGO_URI, appname=APP_NAME, serverSelectionTimeoutMS=TIMEOUT_MS
        )
        # Test the connection
        _client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")

    return _client


def mongo():
    client: MongoClient
    try:
        client = get_client()
    except Exception as e:
        logger.error("❌ Failed to connect to MongoDB:", e)
        raise

    class Collection(BaseModel):
        find_one: Callable[[Dict], Any]
        find_all: Callable[[Dict], List[Any]]
        delete_many: Callable[[Dict], int]

    class Db(BaseModel):
        collection: Callable[[str], Collection]

    class Mongo(BaseModel):
        db: Callable[[str], Db]

    def db(db_name: str):
        db = client[db_name]

        def collection(collection_name: str | AssetType):
            if collection_name in get_args(AssetType):
                collection_name = "cached_" + collection_name
            if collection_name not in db.list_collection_names():
                raise ValueError(
                    f"Collection {collection_name} does not exist in DB {db_name}"
                )
            collection = db[collection_name]

            logger.info(f"Using collection: {collection_name}")

            def find_one(predicate={}):
                doc = collection.find_one(predicate)
                logger.info(f"find_one({predicate}) -> {doc}")
                return doc

            def find_all(predicate={}):
                return list(collection.find(predicate))

            def delete_many(predicate={}) -> int:
                return collection.delete_many(predicate).deleted_count

            return Collection(
                find_one=find_one, find_all=find_all, delete_many=delete_many
            )

        return Db(collection=collection)

    return Mongo(db=db)
