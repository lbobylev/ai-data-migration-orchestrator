import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional
from logger import get_logger

import requests

logger = get_logger(__name__)


IDS: Dict[str, str] = {
    "Eyewear": "upc",
    "CertificationRequest": "requestId",
    "LensDropBallTest": "dropBallTestId",
    "OptiTest": "optiTestId",
    "Organization": "companyId",
}


def id_mapper(asset_type: str) -> str:
    return IDS.get(asset_type, "id")


@dataclass
class ChaincodeApi:
    """Wrapper to call chaincode invoke/query with tag {organization_id, user}."""

    base_url: str
    organization_id: str
    user: str

    def _run(self, tx_type: str, tx_name: str, payload: Optional[dict] = None) -> Any:
        payload = payload or {}
        body = {
            "payload": json.dumps(payload),
            "tag": json.dumps({"organizationId": self.organization_id, "user": self.user}),
        }
        r = requests.post(
            f"{self.base_url}/api/v1.0/chaincode/{tx_type}/{tx_name}",
            json=body,
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        try:
            return json.loads(data.get("data", ""))
        except Exception:
            return data

    def invoke(self, tx_name: str, payload: Optional[dict] = None) -> Any:
        return self._run("invoke", tx_name, payload)

    def query(self, tx_name: str, payload: Optional[dict] = None) -> Any:
        return self._run("query", tx_name, payload)


class BlockchainApi:
    """
    Python version of the JS blockchainApi client.
    """

    def __init__(self, host: str = "localhost", port: int = 3000, dry_run: bool = True) -> None:
        self.base_url = f"http://{host}:{port}"
        self.dry_run = dry_run
        self._cached_assets: Dict[str, List[dict]] = {}
        self._cached_types: Optional[List[str]] = None
        self.ns = "eu.surgetech.ewc.bc.chaincode.model.asset."

    # ----------------- helpers -----------------

    def _request(self, method: str, uri: str, payload: Optional[dict]) -> Any:
        body = {"payload": json.dumps(payload or {})}

        op = payload.get("operation") if payload else None
        if self.dry_run and op in {"DELETE", "DELETE_ALL", "SAVE"}:
            logger.info(f"Dry run: {method} {uri} with payload\n```json\n{json.dumps(body, indent=2)}\n```")
            return None

        r = requests.request(method, uri, json=body, timeout=60)
        r.raise_for_status()
        j = r.json()
        data = j.get("data", "")
        if data == "" and "data" in j:
            return None
        try:
            return json.loads(data)
        except Exception:
            return j

    def _execute(self, type_: str, tx_name: str, payload: dict) -> Any:
        return self._request("POST", f"{self.base_url}/api/v1.0/chaincode/{type_}/{tx_name}", payload)

    def run(self, type_: str, payload: dict) -> Any:
        return self._execute(type_, f"{type_}Direct", payload)

    def run_batch(self, batch_payloads: List[dict]) -> Any:
        return self._execute("invoke", "invokeDirectBatch", {"data": batch_payloads})

    # ----------------- public api -----------------

    def find_all_types(self) -> List[str]:
        res = self._request("POST", f"{self.base_url}/api/v1.0/chaincode/query/findAllTypes", {})
        return (res or {}).get("types", [])

    def find_all(self, type_: str, fields: Optional[List[str]] = None) -> List[dict]:
        res = self.run("query", {"operation": "FIND_ALL", "type": f"{self.ns}{type_}", "fields": fields})
        return [json.loads(x) if isinstance(x, str) else x for x in (res or [])]

    def delete_one(self, type_: str, id_: str) -> Any:
        return self.run("invoke", {"operation": "DELETE", "type": f"{self.ns}{type_}", "id": id_})

    def delete_all(self, type_: str) -> Any:
        return self.run("invoke", {"operation": "DELETE_ALL", "type": f"{self.ns}{type_}"})

    def save(self, type_: str, data: dict) -> Any:
        return self.run(
            "invoke",
            {
                "operation": "SAVE",
                "type": f"{self.ns}{type_}",
                "id": data.get(id_mapper(type_)),
                "data": json.dumps(data),
            },
        )

    def save_batch(self, type_: str, batch: Iterable[dict]) -> Any:
        n = sum(1 for _ in batch)
        if self.dry_run:
            logger.info(f"Dry run: save_batch for {type_} with {n} items")
            return None
        payloads = [
            {"operation": "SAVE", "type": f"{self.ns}{type_}", "id": d.get(id_mapper(type_)), "data": json.dumps(d)}
            for d in batch
        ]
        return self.run_batch(payloads)

    def delete_batch(self, type_: str, ids: Iterable[str]) -> Any:
        n = sum(1 for _ in ids)
        if self.dry_run:
            logger.info(f"Dry run: delete_batch for {type_} with {n} items")
            return None
        payloads = [{"operation": "DELETE", "type": f"{self.ns}{type_}", "id": id_} for id_ in ids]
        return self.run_batch(payloads)

    def exists(self, type_: str, id_: str) -> bool:
        res = self.run("query", {"operation": "EXISTS", "type": f"{self.ns}{type_}", "id": id_})
        return bool((res or {}).get("yes"))

    def find(self, type_: str, id_: str) -> Any:
        res = self.run("query", {"operation": "FIND", "type": f"{self.ns}{type_}", "id": id_})
        return json.loads(res) if isinstance(res, str) else res

    def check_if_referred(self, source_type: str, source_id: str) -> List[str]:
        result: List[str] = []
        if self._cached_types is None:
            self._cached_types = self.find_all_types()

        for fq_type in self._cached_types:
            name = fq_type.split(".")[-1]
            if name not in self._cached_assets:
                self._cached_assets[name] = self.find_all(name)

            for asset in self._cached_assets[name]:
                asset_id = asset.get(id_mapper(name))
                if f"{name}:{asset_id}" == f"{source_type}:{source_id}":
                    continue
                if f"\"{source_id}\"" in json.dumps(asset, ensure_ascii=False):
                    result.append(f"{name}[{asset_id}] refers to {source_type}[{source_id}]")
        return result

    def history(self, type_: str, id_: str) -> Any:
        return self._request(
            "POST",
            f"{self.base_url}/api/v1.0/chaincode/query/queryDirectHistory",
            {"type": f"{self.ns}{type_}", "id": id_},
        )

    def find_all_by_predicate(self, type_: str, predicate: Callable[[dict], bool], fields: Optional[List[str]] = None) -> List[dict]:
        return [x for x in self.find_all(type_, fields) if predicate(x)]

    def chaincode(self, *, organization_id: str, user: str) -> ChaincodeApi:
        return ChaincodeApi(self.base_url, organization_id, user)
