from functools import wraps
import sys
import termios
import tty

disabled = False

def set_confirm_disable(value: bool):
    """Set the disable flag to control confirmation prompts."""
    global disabled
    disabled = value

def _wait_for_space(msg="Push SPACE to continue..."):
    print(msg)
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        while True:
            ch = sys.stdin.read(1)
            if ch == " ":
                break
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

def require_confirm(prompt: str = "Push SPACE to continue..."):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not disabled:
                _wait_for_space(prompt)
            return fn(*args, **kwargs)
        return wrapper
    return decorator

