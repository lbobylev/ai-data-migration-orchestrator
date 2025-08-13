import time
import openai
import random
from typing import (
    List,
    Type,
    TypeVar,
    Any,
    Dict,
    Tuple,
    Iterable,
    Optional,
    Set,
)
from langchain_core.messages import (
    BaseMessage,
    SystemMessage,
    HumanMessage,
)

from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from logger import get_logger

import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from langchain_core.runnables import Runnable
from langchain_core.messages import AIMessage, ToolMessage

logger = get_logger(__name__)


T = TypeVar("T")
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


def _is_transient_error(e: Exception) -> bool:
    return isinstance(
        e, (openai.APITimeoutError, openai.APIConnectionError, openai.Timeout)
    )


def _sleep_backoff(attempt: int, base: float = 0.5, cap: float = 8.0):
    # exp backoff with full jitter
    delay = min(cap, base * (2 ** (attempt - 1)))
    time.sleep(random.uniform(0, delay))


def retry_call(
    runnable: Runnable[InputT, OutputT], *, max_attempts: int = 3
) -> Runnable[InputT, OutputT]:
    class RetryRunnable(Runnable):

        def invoke(self, input: InputT):
            attempt = 1
            while attempt <= max_attempts:
                try:
                    return runnable.invoke(input)
                except Exception as e:
                    if attempt >= max_attempts or not _is_transient_error(e):
                        raise
                    logger.warning(f"Transient error on attempt {attempt}: {e}")
                    _sleep_backoff(attempt)
                    attempt += 1

    return RetryRunnable()


REPAIR_SYSTEM = "You will receive invalid JSON and a schema. Return STRICTLY valid JSON, exactly according to the schema. No comments, explanations, or formatting — only JSON."


STRICT_JSON = (
    "Return STRICTLY raw JSON conforming to the schema. "
    "No code fences, no comments, no prose — JSON only."
)

R = TypeVar("R", bound=BaseModel)


def call_with_self_heal(
    llm: ChatOpenAI,
    messages: List[BaseMessage],
    schema_model: Type[R],
    *,
    max_transient_attempts: int = 3,
    max_repairs: int = 3,
) -> R:
    struct_llm = llm.with_structured_output(schema_model, method="json_mode")

    attempt = 1
    while attempt <= max_transient_attempts:
        try:
            raw = struct_llm.invoke([SystemMessage(content=STRICT_JSON), *messages])
            return schema_model.model_validate(raw)
        except Exception as e:
            if _is_transient_error(e) and attempt < max_transient_attempts:
                logger.warning(f"Transient error attempt {attempt}: {e}")
                _sleep_backoff(attempt)
                attempt += 1
                continue

            last_err = e
            for r in range(max_repairs):
                logger.warning(
                    f"Invalid JSON/validation failed:\n```log\n{last_err}\n```\n\n🔧 Repair attempt: {r+1}"
                )
                repair_msgs = [
                    SystemMessage(content=STRICT_JSON),
                    *messages,
                    HumanMessage(
                        content=f"Previous response was invalid: {last_err}. "
                        f"Please adjust and return valid JSON."
                    ),
                ]
                try:
                    logger.debug(
                        f"Repairing with messages:\n```json\n{json.dumps([{'type': type(m).__name__, 'content': m.content} for m in repair_msgs], indent=2)}\n```"
                    )
                    raw = struct_llm.invoke(repair_msgs)
                    logger.debug(
                        f"Repaired candidate:\n```log\n{raw}\n```"
                    )
                    return schema_model.model_validate(raw)
                except Exception as ve:
                    last_err = ve
                    continue
            raise last_err
    raise RuntimeError("Unreachable")


# ---------- helpers ----------


def _to_text(x: Any, limit: int = 100_000) -> str:
    """Convert any object to string/JSON, truncating if too long."""
    try:
        if isinstance(x, str):
            s = x
        else:
            s = json.dumps(x, ensure_ascii=False, default=str)
    except Exception:
        s = str(x)
    if len(s) > limit:
        return s[:limit] + f"\n...[truncated {len(s)-limit} chars]..."
    return s


def _parse_args(raw: Any) -> Dict[str, Any]:
    """Parse tool call arguments from various formats into a dictionary (always returns dict)."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            val = json.loads(raw)
            return val if isinstance(val, dict) else {"__raw__": val}
        except Exception:
            return {"__raw__": raw}
    try:
        val = json.loads(json.dumps(raw, ensure_ascii=False, default=str))
        return val if isinstance(val, dict) else {"__raw__": val}
    except Exception:
        return {"__raw__": str(raw)}


def _mask(obj: Any, secret_keys: Iterable[str]) -> Any:
    """Mask values for sensitive keys (deep traversal)."""
    keyset = {k.lower() for k in secret_keys}

    def _rec(v):
        if isinstance(v, dict):
            return {
                k: ("***" if k.lower() in keyset else _rec(vv)) for k, vv in v.items()
            }
        if isinstance(v, list):
            return [_rec(i) for i in v]
        return v

    return _rec(obj)


def _norm_json(val: Any) -> str:
    """Stable JSON string for deduplication/comparison."""
    try:
        return json.dumps(val, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        return str(val)


def _calls_signature(calls: List[Dict[str, Any]]) -> Tuple[Tuple[str, str], ...]:
    """Create a stable signature of tool_calls: ((name, normalized_args_json), ...)."""
    sig = []
    for c in calls:
        name = (c.get("name") or "").strip()
        args = c.get("args")
        args = _parse_args(args)
        sig.append((name, _norm_json(args)))
    return tuple(sig)


# ---------- main ----------


def run_with_tools(
    llm: Runnable,  # Runnable[LanguageModelInput, BaseMessage]
    messages: List[BaseMessage],
    tools: List,  # LangChain tools with .name and .invoke
    *,
    max_rounds: int = 8,
    max_tools_per_round: int = 5,
    max_total_tool_calls: int = 20,
    overall_timeout_s: Optional[float] = 60.0,
    per_tool_timeout_s: Optional[float] = 15.0,
    secret_keys: Iterable[str] = (
        "token",
        "password",
        "api_key",
        "authorization",
        "secret",
    ),
    log_arg_limit: int = 300,
    dedupe_same_call: bool = True,
) -> Tuple[AIMessage, List[BaseMessage]]:
    """
    Run LLM with tools in a controlled loop.

    Safety features:
      - max_rounds / overall_timeout
      - limit tools per round and total tool calls
      - deduplicate repeated tool calls
      - early stop if identical tool_calls repeat (looping)
      - per-tool timeout handling
      - sensitive args masked in logs
    """
    tool_map = {t.name: t for t in tools}
    start_t = time.monotonic()
    rounds = 0
    total_tool_calls = 0
    prev_sig: Optional[Tuple[Tuple[str, str], ...]] = None
    seen_calls: Set[Tuple[str, str]] = set()

    def _invoke_tool_with_timeout(tool, args):
        """Invoke a tool with optional timeout protection."""
        if per_tool_timeout_s is None or per_tool_timeout_s <= 0:
            return tool.invoke(args)
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(tool.invoke, args)
            return fut.result(timeout=per_tool_timeout_s)

    while True:
        rounds += 1
        if (
            overall_timeout_s is not None
            and (time.monotonic() - start_t) > overall_timeout_s
        ):
            logger.warning("[run_with_tools] overall timeout reached")
            ai: AIMessage = llm.invoke(messages)  # final attempt without tools
            messages.append(ai)
            return ai, messages

        logger.info(f"[run_with_tools] round={rounds} messages={len(messages)}")
        ai: AIMessage = llm.invoke(messages)  # type: ignore
        messages.append(ai)
        logger.info(f"[run_with_tools] AI: {_to_text(ai.content, 200)}")

        # Normalize tool_calls
        calls = getattr(ai, "tool_calls", None) or []
        norm_calls = []
        for c in calls:
            if isinstance(c, dict):
                norm_calls.append(c)
            else:
                try:
                    norm_calls.append(dict(c))
                except Exception:
                    logger.debug(
                        f"[run_with_tools] unknown tool_call shape: {type(c)} -> {c}"
                    )
        if not norm_calls:
            logger.info("[run_with_tools] no tool calls; returning")
            return ai, messages

        # Early stop: if same set of tool_calls as previous round
        sig = _calls_signature(norm_calls)
        if prev_sig is not None and sig == prev_sig:
            logger.warning("[run_with_tools] repeated tool_calls detected; stopping")
            return ai, messages
        prev_sig = sig

        # Limit tools per round
        if len(norm_calls) > max_tools_per_round:
            logger.warning(
                f"[run_with_tools] truncating tool_calls {len(norm_calls)} -> {max_tools_per_round}"
            )
            norm_calls = norm_calls[:max_tools_per_round]

        # Execute tools
        for call in norm_calls:
            if total_tool_calls >= max_total_tool_calls:
                logger.warning(
                    "[run_with_tools] max_total_tool_calls reached; breaking"
                )
                break

            name = (call.get("name") or "").strip()
            call_id = call.get("id")
            args = _parse_args(call.get("args"))

            # Deduplication across rounds
            call_key = (name, _norm_json(args))
            if dedupe_same_call and call_key in seen_calls:
                logger.info(f"[TOOL SKIP] duplicate call {name} {call_key[1][:120]}...")
                messages.append(
                    ToolMessage(
                        content=_to_text(
                            {"status": "skipped", "reason": "duplicate_call"}
                        ),
                        tool_call_id=call_id,
                    )
                )
                continue
            seen_calls.add(call_key)

            masked_args = _mask(args, secret_keys)
            logger.info(
                f"[TOOL START] {name} args={_to_text(masked_args, log_arg_limit)}"
            )

            if name not in tool_map:
                err = f"Unknown tool '{name}'. Available: {list(tool_map.keys())}"
                logger.info(f"[TOOL ERROR] {name}: {err}")
                tool_output = {
                    "status": "error",
                    "error": err,
                    "retryable": False,
                    "code": "UNKNOWN_TOOL",
                }
            else:
                tool = tool_map[name]
                try:
                    tool_output = _invoke_tool_with_timeout(tool, args)
                    logger.debug(f"[TOOL OUTPUT] {name}: {_to_text(tool_output, 500)}")
                    tool_output = {"status": "ok", "result": tool_output}
                except FutureTimeout:
                    logger.exception(f"[TOOL TIMEOUT] {name} > {per_tool_timeout_s}s")
                    tool_output = {
                        "status": "error",
                        "error": f"Tool '{name}' timed out after {per_tool_timeout_s}s",
                        "retryable": True,
                        "code": "TIMEOUT",
                    }
                except Exception as e:
                    logger.exception(f"[TOOL EXC] {name}: {e}")
                    tool_output = {
                        "status": "error",
                        "error": str(e),
                        "retryable": True,
                        "code": "EXCEPTION",
                    }

            messages.append(
                ToolMessage(
                    content=_to_text(tool_output, 10_000),
                    tool_call_id=call_id,
                )
            )
            total_tool_calls += 1

        # Stop if rounds exceeded
        if rounds >= max_rounds:
            logger.warning("[run_with_tools] max_rounds reached; stopping")
            ai_final: AIMessage = llm.invoke(messages)  # one last try to summarize
            messages.append(ai_final)
            return ai_final, messages
