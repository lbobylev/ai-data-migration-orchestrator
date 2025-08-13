from datetime import datetime
import os
import shutil
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Literal, Optional

from app_types import Environment
from logger import get_logger
from sh_utils import check_cmd_exists, is_port_in_use, run_cmd
from rapidfuzz import process, fuzz

logger = get_logger(__name__)

Namespace = Literal[
    "adm-sftp-server",
    "ai",
    "backup",
    "cert-manager",
    "cpbc-dev-fab",
    "default",
    "e2e-poc",
    "flux-system",
    "kering-dev",
    "keycloak",
    "kube-node-lease",
    "kube-public",
    "kube-system",
    "monitoring",
    "nginx-ingress",
    "picture",
    "qa",
    "shared-dev",
    "shared-dev-fab",
    "sonarqube",
    "trivy-system",
    "vm-dev",
    "cpbc-test-fab",
    "harbor",
    "kering-test",
    "kering-test-fab",
    "shared-test",
    "shared-test-fab",
    "vm-test",
    "aks-command",
    "cpbc-preprod-fab",
    "kering-preprod",
    "kering-preprod-fab",
    "shared-preprod",
    "shared-preprod-fab",
    "vm-preprod",
    "cpbc-prod-fab",
    "kering-prod",
    "kering-prod-fab",
    "shared-prod",
    "shared-prod-fab",
    "vm-prod",
]

NAMESPACES_BY_ENV: Dict[Environment, List[Namespace]] = {
    "dev": ["shared-dev-fab", "e2e-poc", "shared-dev"],
    "test": ["shared-test-fab", "kering-test-fab", "shared-test", "kering-test"],
    "preprod": [
        "shared-preprod-fab",
        "kering-preprod-fab",
        "shared-preprod",
        "kering-preprod",
    ],
    "prod": ["shared-prod-fab", "kering-prod-fab", "shared-prod", "kering-prod"],
}


class KubeError(RuntimeError):
    pass


class PortForwardError(KubeError):
    pass


@dataclass
class PortForwardHandle:
    process: subprocess.Popen
    log_path: Path
    env: Environment
    ns: str


def maybe_connect_vpn(kube_context: str, vpn_name: str = "DedicatedVPN") -> None:
    if kube_context == "dev":
        return
    vpnutil = shutil.which("vpnutil")
    if vpnutil:
        logger.info("Connecting to VPN using vpnutil...")
        try:
            run_cmd([vpnutil, "start", vpn_name])
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to start VPN ({e}). Continuing without VPN.")
    else:
        logger.info("vpnutil not found, skipping VPN connection step.")


def _current_kube_context() -> str:
    try:
        out = run_cmd(
            ["kubectl", "config", "current-context"], capture_output=True
        ).stdout.strip()
        return out
    except subprocess.CalledProcessError:
        return ""


def get_logs(
    pod_name: str,
    env: Environment,
    namespace: Namespace,
    *,
    timeout=10,
    since_time: Optional[str] = None,
) -> str:
    """
    Return the logs of the specified pod.
    """
    cmd = ["kubectl", "logs", "-n", namespace, pod_name]
    if since_time is not None:
        cmd += ["--since-time", since_time]
    logger.debug(f"Running command: {' '.join(cmd)}")
    try:
        switch_context(env)
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout
    except FileNotFoundError as e:
        raise KubeError("kubectl not found. Please install kubectl.") from e
    except subprocess.TimeoutExpired as e:
        raise KubeError("kubectl command timed out.") from e
    except subprocess.CalledProcessError as e:
        raise KubeError(f"kubectl failed: {e}") from e


def find_pod_name_fuzzy(
    pod_name: str,
    environment: Environment,
    namespace: Namespace,
    *,
    min_score=90,
    timeout=10,
    since_time: Optional[str] = None,  # ISO format string
) -> Optional[str]:
    """
    Return the name of a pod that fuzzy-matches `name` with at least 90% similarity.
    """
    logger.debug(f"Finding pod name like '{pod_name}' in {environment}/{namespace}")

    try:
        switch_context(environment)
        result = subprocess.run(
            ["kubectl", "get", "po", "-n", namespace],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as e:
        raise KubeError("kubectl not found. Please install kubectl.") from e
    except subprocess.TimeoutExpired as e:
        raise KubeError("kubectl command timed out.") from e
    except subprocess.CalledProcessError as e:
        raise KubeError(f"kubectl failed: {e}") from e

    pod_names = [line.split()[0] for line in result.stdout.splitlines()[1:]]
    matches = process.extract(
        pod_name,
        pod_names,
        scorer=fuzz.WRatio,
        score_cutoff=min_score,
        limit=2,
    )
    if matches:
        if len(matches) > 1:
            logger.warning(
                f"Multiple matches found for pod name '{pod_name}': {matches}"
            )
        best_match = matches[0]
        logger.debug(f"Best match: {best_match}")
        return best_match[0]

    return None


def _get_bcrest_pod_name(env: str, ns: str) -> str:
    """
    Return the first pod name in namespace `shared-<env>-fab` that contains 'bcrest'.
    Raises RuntimeError if no pod is found.
    """
    namespace = f"shared-{env}-fab" if ns is None else ns
    try:
        # Run kubectl and capture output
        result = subprocess.run(
            ["kubectl", "-n", namespace, "get", "po"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise PortForwardError(f"kubectl failed: {e}") from e

    for line in result.stdout.splitlines():
        if "bcrest" in line:
            # awk '{print $1}' equivalent = first whitespace-separated token
            pod_name = line.split()[0]
            return pod_name

    raise PortForwardError(f"No bcrest pod found in namespace {namespace}")


def switch_context(environment: Environment) -> None:
    kube_context = "shared" if environment == "prod" else environment

    maybe_connect_vpn(kube_context)

    current = _current_kube_context()
    if current == kube_context:
        logger.info(f"Already in kube-context: {kube_context}")
    else:
        logger.info(f"Switching context {kube_context}")
        subscription = "dev" if kube_context == "dev" else "prod"
        check_cmd_exists("az", "Azure CLI 'az' is required.")
        check_cmd_exists("kubectx", "'kubectx' is required to switch contexts.")
        run_cmd(["az", "account", "set", f"--subscription={subscription}"])
        run_cmd(["kubectx", kube_context])
        now = _current_kube_context()
        logger.debug(f"Now using context {now}")


def start_port_forwarding(
    environment: Environment,
    *,
    namespace: Optional[Namespace] = None,
    port: int = 3000,
    log_file: str = "port-forward.log",
    wait_seconds: float = 3.0,
) -> PortForwardHandle:
    check_cmd_exists("kubectl", "'kubectl' is required. Please install it.")
    switch_context(environment)

    ns = f"shared-{environment}-fab" if namespace is None else namespace

    pod_name = _get_bcrest_pod_name(environment, ns=ns)
    logger.info(f"Found bcrest pod: {pod_name}")

    if is_port_in_use(port):
        raise PortForwardError(
            f"Port {port} is already in use. Please choose another port."
        )
    
    logger.info(f"Forwarding port {port} -> {pod_name}:8080 in namespace {namespace}")

    log_path = Path(log_file).resolve()
    log_fh = open(log_path, "w")
    proc = subprocess.Popen(
        ["kubectl", "-n", ns, "port-forward", pod_name, f"{port}:8080"],
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )

    try:
        if wait_seconds > 0:
            proc.wait(timeout=wait_seconds)
            rc = proc.returncode
            if rc is not None:
                log_fh.close()
                with open(log_path, "r") as f:
                    log_tail = f.read()[-2000:]
                raise PortForwardError(
                    f"port-forward exited too early (rc={rc}). Log:\n{log_tail}"
                )
    except subprocess.TimeoutExpired:
        pass

    return PortForwardHandle(process=proc, log_path=log_path, env=environment, ns=ns)


def stop_port_forwarding(handle: PortForwardHandle) -> None:
    proc = handle.process
    if proc.poll() is not None:
        return

    logger.info(f"Stopping port-forward (pid: {proc.pid})...")
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    except Exception as e:
        try:
            proc.terminate()
        except Exception:
            try:
                proc.kill()
            except Exception:
                logger.error(f"Failed to terminate process: {e}")

    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)
        except Exception:
            pass
