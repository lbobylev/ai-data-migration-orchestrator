import subprocess
import atexit
from pymongo import MongoClient
from typing import Literal, TypedDict, List

MONGO_URI = "mongodb://localhost:27017"
APP_NAME = "surge-agent"
TIMEOUT_MS = 5000

_port_forward_started = False
_port_forward_process = None
_client = None


def _start_port_forward():
    """
    Запускает kubectl port-forward в фоне, если ещё не запущен.
    """
    global _port_forward_started, _port_forward_process

    if _port_forward_started:
        return

    print("🚀 Запуск kubectl port-forward...")
    cmd = [
        "kubectl", "-n", "shared-dev", "port-forward",
        "shared-mongo-mongodb-0", "27017:27017"
    ]
    # Запускаем как фон, stdout/stderr перенаправляем, чтобы не блокировать
    _port_forward_process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    _port_forward_started = True

    # Регистрируем завершение процесса при выходе
    atexit.register(_stop_port_forward)


def _stop_port_forward():
    """
    Останавливает kubectl port-forward при выходе из программы.
    """
    global _port_forward_process
    if _port_forward_process and _port_forward_process.poll() is None:
        print("🛑 Остановка kubectl port-forward...")
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
        _start_port_forward()
        # Тут можно подождать пару секунд, чтобы портфорвард поднялся
        import time
        time.sleep(2)

        _client = MongoClient(
            MONGO_URI,
            appname=APP_NAME,
            serverSelectionTimeoutMS=TIMEOUT_MS
        )
        # Проверим соединение
        _client.admin.command("ping")
        print("✅ Подключено к MongoDB через port-forward")

    return _client

def find_one(collection_name: str, predicate = {}, db_name = "kering") -> dict | None:
    client = get_client()
    db = client[db_name]
    collection = db[collection_name]

    return collection.find_one(predicate)

def find_all(collection_name: str, predicate = {}, db_name = "kering") -> List[dict]:
    client = get_client()
    db = client[db_name]
    collection = db[collection_name]

    return list(collection.find(predicate))
