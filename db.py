import subprocess
import atexit
from typing import Any, Callable, Dict, List, get_args
from pydantic import BaseModel
from pymongo import MongoClient

from app_types import AssetType, Environment
from bc.kube_utils import switch_context
from logger import get_logger

MONGO_URI = "mongodb://localhost:27017"
APP_NAME = "surge-agent"
TIMEOUT_MS = 5000

_port_forward_started = False
_port_forward_process = None
_client = None

logger = get_logger()


def start_port_forward(env: Environment):
    """
    Launches kubectl port-forward in the background if it is not already running.
    """
    global _port_forward_started, _port_forward_process

    if _port_forward_started:
        return

    switch_context(env)

    namespace = "shared" if env == "dev" else "kering"

    logger.info(f"🚀 Starting mongo port-forward for env: {env}...")
    cmd = [
        "kubectl",
        "-n",
        f"{namespace}-{env}",
        "port-forward",
        f"{namespace}-mongo-mongodb-0",
        "27017:27017",
    ]
    # Run it in the background, redirect stdout and stderr so it doesn't block
    _port_forward_process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    _port_forward_started = True

    # Register the process termination on exit
    atexit.register(stop_port_forward)


def stop_port_forward():
    """
    Stops the kubectl port-forward.
    """
    global _port_forward_process
    if _port_forward_process and _port_forward_process.poll() is None:
        logger.info("🛑 Stop mongo port-forward...")
        _port_forward_process.terminate()
        try:
            _port_forward_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _port_forward_process.kill()


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

    class Db(BaseModel):
        collection: Callable[[str], Collection]

    class Mongo(BaseModel):
        db: Callable[[str], Db]

    def db(db_name: str):
        db = client[db_name]

        def collection(collection_name: str | AssetType):
            collection = db[collection_name]

            logger.info(f"Using collection: {collection_name}")

            def find_one(predicate={}):
                doc = collection.find_one(predicate)
                logger.info(f"find_one({predicate}) -> {doc}")
                return doc

            def find_all(predicate={}):
                return list(collection.find(predicate))

            return Collection(find_one=find_one, find_all=find_all)

        return Db(collection=collection)

    return Mongo(db=db)
