from dotenv import load_dotenv
from langchain_core.callbacks import BaseCallbackHandler
from typing import Protocol, Any
import logging
import shutil
import subprocess
import os

from tenacity import RetryCallState


class LoggerInterface(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


def _has_glow():
    return shutil.which("glow") is not None


class GlowHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        subprocess.run(["glow", "-"], input=msg.encode(), check=True)


def get_logger(name: str = __name__) -> LoggerInterface:
    load_dotenv()
    logger = logging.getLogger(name)
    logging_level = os.getenv("LOGGING_LEVEL", "DEBUG").upper()
    logger.setLevel(logging_level)

    if not logger.handlers:  # Prevent duplicate handlers if called multiple times
        handler = GlowHandler() if _has_glow() else logging.StreamHandler()
        #handler.setLevel(logging_level)
        glow_fmt = """
# %(asctime)s [%(levelname)s] %(name)s
%(message)s
        """
        fmt = (
            glow_fmt
            if _has_glow()
            else "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )

        formatter = logging.Formatter(
            fmt=fmt,
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


logger = get_logger("graph_logger")


class AppLogger(BaseCallbackHandler):
    def on_tool_start(self, tool, input_str, **kwargs):
        tool_name = tool["name"]
        input_str_truncated = input_str[:300]
        logger.info(
            f"[TOOL START] {tool_name} with input:\n```log{input_str_truncated}...\n```"
        )

    def on_tool_end(self, output, **kwargs):
        # print(f"[TOOL END] Output: {output}")
        logger.info("[TOOL END]")

    def on_retry(
        self,
        retry_state: RetryCallState,
        *,
        run_id=None,
        parent_run_id=None,
        **kwargs,
    ) -> None:
        err = retry_state.outcome.exception() if retry_state.outcome else None
        logger.error(f"Retrying due to error: {err}")

    # def on_llm_start(self, serialized, prompts, **kwargs):
    #     truncated_prompts = [prompt[:300] for prompt in prompts]
    #     print(f"[LLM START] Prompts:\n{truncated_prompts}")
    #
    # def on_llm_end(self, response, **kwargs):
    #     truncated_response = response[:300] if isinstance(response, str) else response
    #     print(f"[LLM END] Response:\n{truncated_response}")
