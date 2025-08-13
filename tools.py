from __future__ import annotations
from typing import List, Optional
import json


import os
import tempfile
import textwrap
import time

import docker
from docker.models.containers import Container  # explicit import in docker 7.x
from pydantic import BaseModel, Field, field_validator, ConfigDict
from langchain_core.tools import tool

# ---------- helpers ----------


def _nano_cpus(cores: float) -> int:
    return int(cores * 1e9)


def _write_code(tmpdir: str, code: str) -> str:
    os.makedirs(tmpdir, exist_ok=True)
    path = os.path.join(tmpdir, "main.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(code))
    return path


def _pip_install_into_venv(
    client: docker.DockerClient,
    workspace: str,
    base_image: str,
    requirements: List[str],
    mem_limit_mb: int,
    cpu_limit_cores: float,
    timeout_seconds: int = 60,
) -> None:
    """Create /workspace/.venv and pip install requirements (network enabled only here)."""
    req_str = " ".join(requirements)
    cmd = [
        "bash",
        "-lc",
        (
            "python -m venv /workspace/.venv && "
            "/workspace/.venv/bin/python -m pip install --upgrade pip && "
            f"/workspace/.venv/bin/pip install {req_str}"
        ),
    ]

    c: Optional[Container] = None
    try:
        c = client.containers.run(
            image=base_image,
            command=cmd,
            working_dir="/workspace",
            volumes={workspace: {"bind": "/workspace", "mode": "rw"}},
            network_disabled=False,  # only here
            mem_limit=f"{mem_limit_mb}m",
            nano_cpus=_nano_cpus(cpu_limit_cores),
            detach=True,
            stdout=True,
            stderr=True,
            tty=False,
            environment={"PYTHONUNBUFFERED": "1"},
            user="1000:1000",
            security_opt=["no-new-privileges:true"],
            cap_drop=["ALL"],
        )
        status = c.wait(timeout=timeout_seconds)
        if status.get("StatusCode", 1) != 0:
            logs = c.logs(stdout=True, stderr=True).decode("utf-8", "replace")
            raise RuntimeError(f"pip install failed:\n{logs}")
    finally:
        if c is not None:
            try:
                c.remove(force=True)
            except Exception:
                pass


# ---------- schema ----------


class RunPythonInDockerInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(..., description="Python code to run inside Docker")
    requirements: Optional[List[str]] = Field(
        default=None, description="Optional pip packages to install before running"
    )
    timeout_seconds: int = Field(
        20, ge=1, le=300, description="Max execution time (seconds)"
    )
    mem_limit_mb: int = Field(512, ge=64, le=8192, description="RAM limit (MiB)")
    cpu_limit_cores: float = Field(1.0, ge=0.1, le=4.0, description="CPU limit (cores)")

    @field_validator("code")
    @classmethod
    def _sanitize_and_limit(cls, v: str) -> str:
        if len(v) > 200_000:
            raise ValueError("Code too large (>200k chars)")
        return v.replace("\ufeff", "").strip()


# ---------- tool ----------


@tool("run_python_in_docker", args_schema=RunPythonInDockerInput)
def run_python_in_docker(
    code: str,
    requirements: Optional[List[str]] = None,
    timeout_seconds: int = 20,
    mem_limit_mb: int = 512,
    cpu_limit_cores: float = 1.0,
) -> str:
    """
    Execute Python code in a sandboxed Docker container.

    Returns JSON: {stdout, stderr, exit_code, duration_sec, timed_out}
    """
    client = docker.from_env()
    base_image = "python:3.11-slim"
    tmpdir = tempfile.mkdtemp(prefix="lc_pycode_")
    _write_code(tmpdir, code)

    # Optional deps install into a venv (separate step, network on)
    if requirements:
        _pip_install_into_venv(
            client=client,
            workspace=tmpdir,
            base_image=base_image,
            requirements=requirements,
            mem_limit_mb=mem_limit_mb,
            cpu_limit_cores=cpu_limit_cores,
            timeout_seconds=max(10, min(180, timeout_seconds)),  # simple bound
        )

    # Choose interpreter: prefer venv if created
    python_bin = "/workspace/.venv/bin/python"
    use_venv = os.path.exists(os.path.join(tmpdir, ".venv", "bin", "python"))
    cmd = [python_bin if use_venv else "python", "/workspace/main.py"]

    container: Optional[Container] = None
    start = time.time()
    timed_out = False
    exit_code: int = -1
    stdout = ""
    stderr = ""

    try:
        container = client.containers.run(
            image=base_image,
            command=cmd,
            working_dir="/workspace",
            volumes={tmpdir: {"bind": "/workspace", "mode": "ro"}},
            tmpfs={"/tmp": ""},  # writable /tmp
            read_only=True,  # read-only rootfs
            network_disabled=True,  # no network for user code
            mem_limit=f"{mem_limit_mb}m",
            nano_cpus=_nano_cpus(cpu_limit_cores),
            detach=True,
            stdout=True,
            stderr=True,
            tty=False,  # <-- crucial for capturing logs
            environment={"PYTHONUNBUFFERED": "1"},
            user="1000:1000",
            security_opt=["no-new-privileges:true"],
            cap_drop=["ALL"],
        )

        try:
            exit_code = container.wait(timeout=timeout_seconds).get("StatusCode", -1)
        except Exception:
            timed_out = True
            try:
                container.kill()
            finally:
                exit_code = -1

        # Single, simple path: combined logs; split after the fact if needed.
        logs_bytes = container.logs(stdout=True, stderr=True)
        logs = logs_bytes.decode("utf-8", "replace")
        # naive split: keep everything in stdout; preserve stderr empty for simplicity
        # (usually fine; if you need strict split, reintroduce demux)
        stdout, stderr = logs, ""

    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception:
                pass

    duration = round(time.time() - start, 3)
    return json.dumps(
        {
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "duration_sec": duration,
        },
        ensure_ascii=False,
    )
