import shutil
import socket
import subprocess
from typing import Optional

class ShellError(RuntimeError):
    pass


def check_cmd_exists(cmd: str, msg: Optional[str] = None) -> None:
    if shutil.which(cmd) is None:
        raise ShellError(msg or f"Required command '{cmd}' is not found in PATH.")


def run_cmd(
    cmd, *, check=True, capture_output=False, text=True, env=None
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, check=check, capture_output=capture_output, text=text, env=env
    )


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return True
    return False
