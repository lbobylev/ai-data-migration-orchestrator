import os
import shutil
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path

from app_types import Environment
from logger import get_logger
from sh_utils import check_cmd_exists, is_port_in_use, run_cmd

logger = get_logger(__name__)


class PortForwardError(RuntimeError):
    pass


@dataclass
class PortForwardHandle:
    process: subprocess.Popen
    log_path: Path


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


def _get_bcrest_pod_name(env: str) -> str:
    """
    Return the first pod name in namespace `shared-<env>-fab` that contains 'bcrest'.
    Raises RuntimeError if no pod is found.
    """
    namespace = f"shared-{env}-fab"
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
        logger.info(f"Switching kube-context to: {kube_context}")
        subscription = "dev" if kube_context == "dev" else "prod"
        check_cmd_exists("az", "Azure CLI 'az' is required.")
        check_cmd_exists("kubectx", "'kubectx' is required to switch contexts.")
        run_cmd(["az", "account", "set", f"--subscription={subscription}"])
        run_cmd(["kubectx", kube_context])
        now = _current_kube_context()
        logger.info(f"Now using kube-context: {now}")


def start_port_forwarding(
    environment: Environment,
    port: int = 3000,
    *,
    log_file: str = "port-forward.log",
    wait_seconds: float = 3.0,
) -> PortForwardHandle:
    check_cmd_exists("kubectl", "'kubectl' is required. Please install it.")
    switch_context(environment)

    pod_name = _get_bcrest_pod_name(environment)
    logger.info(f"Found bcrest pod: {pod_name}")

    if is_port_in_use(port):
        raise PortForwardError(
            f"Port {port} is already in use. Please choose another port."
        )

    namespace = f"shared-{environment}-fab"
    logger.info(f"Forwarding port {port} -> {pod_name}:8080 in namespace {namespace}")

    log_path = Path(log_file).resolve()
    log_fh = open(log_path, "w")
    proc = subprocess.Popen(
        ["kubectl", "-n", namespace, "port-forward", pod_name, f"{port}:8080"],
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

    return PortForwardHandle(process=proc, log_path=log_path)


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
