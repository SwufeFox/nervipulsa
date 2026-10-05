"""LLM actor: one in-flight model request, batches, and an activation ledger.

Tool calls are submitted, not awaited. Acceptance receipts stay in the
transcript; python.finished arrives later as a runtime observation.

ContextProjector builds the model view. Execution records stay in the
transcript archive and the journal; the Python worker owns the namespace;
live unfinished work is read from Runtime, not from a summary.
"""

from __future__ import annotations

import asyncio
import json
import math
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from .events import MAX_HANDLER_RESULTS_PER_EXECUTION, Emitter, Event, Lane, Mailbox, RuntimeState
from .providers import (
    SYSTEM_PROMPT,
    ModelRequest,
    ModelResponse,
    ProviderError,
    ToolCall,
    environment_tool_schema,
    tool_schema,
)
from .python_environment import discover_python_environment

# Room left for a summary before the view is treated as near the budget.
SUMMARY_RESERVE = 900
SUMMARY_INSTRUCTION = (
    "Summarize these earlier coding-agent interactions for a later turn. "
    "Preserve user requirements, later modifications, files touched, execution ids, "
    "observed statuses, errors, and output-file paths. "
    "Do not say the task is currently finished. Do not invent results. "
    "Do not suggest re-running completed code. "
    "Reply with the summary text only, in at most 120 words."
)


@dataclass
class Activation:
    id: str
    input_event_ids: tuple[str, ...]
    transcript_version: int
    status: str
    input_high_water_seq: int
    tool_calls: dict[str, str] = field(default_factory=dict)
    snapshot: list[dict[str, Any]] | None = None
    response: ModelResponse | None = None
    response_recorded: bool = False
    feedback_sent: bool = False
    error_kind: str | None = None
    error_message: str | None = None
    started_at: float = 0.0
    ended_at: float | None = None
    context_metrics: dict[str, Any] = field(default_factory=dict)


class Transcript:
    """Append-only model view with exact incremental size accounting.

    json.dumps joins list items with the two-character separator ", ", so the
    serialized length of a snapshot is exactly
    `base + sum(len(dumps(message))) + 2 * len(messages)`.
    Per-index sizes are cached, so a measurement never re-dumps the whole
    history. External mutation is detected by a shape signature and repaired by
    resyncing once, keeping public list attributes usable by existing callers.
    """

    _SEPARATOR_CHARS = 2  # json.dumps list separator ", "

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.event_ids: list[str | None] = []
        self.event_types: list[str | None] = []
        self.seen: set[str] = set()
        self.version = 0
        self.archive: list[dict[str, Any]] = []
        system = {"role": "system", "content": SYSTEM_PROMPT}
        self._base_chars = len(json.dumps([system], ensure_ascii=False))
        self._sizes: list[int] = []
        self._size_sum = 0
        self._signature = (0, 0)

    # --- size accounting -------------------------------------------------

    @staticmethod
    def _size_of(message: dict[str, Any]) -> int:
        return len(json.dumps(message, ensure_ascii=False))

    def _rebuild_sizes(self) -> None:
        self._sizes = [self._size_of(message) for message in self.messages]
        self._size_sum = sum(self._sizes)
        self._signature = (len(self.messages), len(self.event_ids))

    def _sync_sizes(self) -> None:
        """Detect mutation that bypassed the append helpers and resync once."""
        if self._signature != (len(self.messages), len(self.event_ids)):
            self._rebuild_sizes()

    def _index_cost(self, indices) -> int:
        self._sync_sizes()
        return sum(self._sizes[index] + self._SEPARATOR_CHARS for index in indices)

    def serialized_length(self) -> int:
        self._sync_sizes()
        return self._base_chars + self._size_sum + self._SEPARATOR_CHARS * len(self.messages)

    def project(self, event: Event) -> dict[str, Any]:
        if event.id in self.seen:
            return {"projected": False, "duplicate": True, "added_chars": 0}
        self.seen.add(event.id)
        if event.type == "agent.retry":
            return {"projected": False, "duplicate": False, "added_chars": 0}
        if event.type == "user.message":
            message = {"role": "user", "content": str(event.payload["text"])}
        elif event.type in {"python.finished", "python.environment_discovered", "agent.feedback", "python.environment_changed", "agent.handler_fired"}:
            envelope = {
                "kind": "runtime_event",
                "source": event.source,
                "event_id": event.id,
                "type": event.type,
                "reply_to": event.reply_to,
                "payload": dict(event.payload),
            }
            message = {
                "role": "user",
                "content": json.dumps(envelope, ensure_ascii=False, separators=(",", ":")),
            }
        else:
            return {"projected": False, "duplicate": False, "added_chars": 0}
        added_chars = self._append(message, event.id, event.type)
        return {"projected": True, "duplicate": False, "added_chars": added_chars}

    def snapshot(self) -> list[dict[str, Any]]:
        return [{"role": "system", "content": SYSTEM_PROMPT}, *self.messages]

    def add_assistant(self, response: ModelResponse) -> None:
        message: dict[str, Any] = {"role": "assistant", "content": response.content or ""}
        if response.tool_calls:
            message["tool_calls"] = [
                {
                    "id": call.id,
                    "type": "function",
                    "function": {
                        "name": call.name,
                        "arguments": json.dumps(call.arguments or {}, ensure_ascii=False),
                    },
                }
                for call in response.tool_calls
            ]
        if response.reasoning_content and response.tool_calls:
            # Providers such as DeepSeek require the tool-call turn to carry its
            # own reasoning_content; plain turns are sent without it.
            message["reasoning_content"] = response.reasoning_content
        self._append(message)

    def add_tool(self, tool_call_id: str, receipt: dict[str, Any]) -> None:
        self._append(
            {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": json.dumps(receipt, ensure_ascii=False, separators=(",", ":")),
            }
        )

    def compact(self, *, current_event_ids: set[str], limit: int) -> dict[str, Any]:
        before = self.serialized_length()
        result = {
            "chars_before": before,
            "chars_after": before,
            "chars_recovered": 0,
            "completed_tool_interactions": 0,
            "plain_turns": 0,
            "pending_tool_interactions_kept": 0,
        }
        if before <= limit:
            return result

        groups = self._tool_interactions()
        protected = self._protected_indices(current_event_ids)
        recent_groups = {id(group) for group in groups[-3:]}
        failures = [
            group
            for group in groups
            if any(item.get("status") not in {"succeeded", "rejected"} for item in group["results"])
        ]
        recent_failures = {id(group) for group in failures[-3:]}
        candidates: list[dict[str, Any]] = []
        for group in groups:
            indices = group["indices"]
            if group["pending"]:
                result["pending_tool_interactions_kept"] += 1
                protected.update(indices)
            elif id(group) in recent_groups or id(group) in recent_failures or indices & protected:
                protected.update(indices)
            else:
                candidates.append(group)

        # Select enough complete groups first, then rebuild once so async result
        # messages may be removed without invalidating the other group indexes.
        selected: list[tuple[dict[str, Any], dict[str, Any]]] = []
        estimated_chars = before
        for group in candidates:
            indices = group["indices"]
            summary = self._tool_summary(group)
            removed_chars = self._index_cost(indices)
            saved = (
                removed_chars
                - len(json.dumps(summary, ensure_ascii=False))
                + self._SEPARATOR_CHARS * (len(indices) - 1)
            )
            if saved <= 0:
                continue
            selected.append((group, summary))
            estimated_chars -= saved
            if estimated_chars <= limit:
                break
        if selected:
            removed_indices: set[int] = set()
            summaries: dict[int, dict[str, Any]] = {}
            for group, summary in selected:
                indices = group["indices"]
                removed_indices.update(indices)
                summaries[min(indices)] = summary
            messages: list[dict[str, Any]] = []
            sizes: list[int] = []
            event_ids: list[str | None] = []
            event_types: list[str | None] = []
            for index, message in enumerate(self.messages):
                if index in summaries:
                    messages.append(summaries[index])
                    sizes.append(self._size_of(summaries[index]))
                    event_ids.append(None)
                    event_types.append(None)
                elif index in removed_indices:
                    continue
                else:
                    messages.append(message)
                    sizes.append(self._sizes[index])
                    event_ids.append(self.event_ids[index])
                    event_types.append(self.event_types[index])
            self.messages = messages
            self._sizes = sizes
            self._size_sum = sum(sizes)
            self.event_ids = event_ids
            self.event_types = event_types
            self._signature = (len(messages), len(event_ids))
            result["completed_tool_interactions"] = len(selected)
            result["chars_after"] = self.serialized_length()

        if result["chars_after"] > limit:
            for indices, summary in reversed(self._plain_turns(current_event_ids)):
                if result["chars_after"] <= limit:
                    break
                self._replace_indices(indices, indices[0], summary)
                result["plain_turns"] += 1
                result["chars_after"] = self.serialized_length()

        if result["completed_tool_interactions"] or result["plain_turns"]:
            self.version += 1
        result["chars_recovered"] = before - result["chars_after"]
        return result

    def plan_compaction(self, *, current_event_ids: set[str], limit: int) -> dict[str, Any]:
        """Choose a prefix of complete fragments. Does not mutate the record."""
        before = self.serialized_length()
        plan = {
            "chars_before": before,
            "remove_indices": [],
            "insert_at": None,
            "summary_input": [],
            "completed_tool_interactions": 0,
            "plain_turns": 0,
            "pending_tool_interactions_kept": 0,
        }
        if before <= limit:
            return plan

        groups = self._tool_interactions()
        protected = {
            index
            for index, event_id in enumerate(self.event_ids)
            if event_id is not None and event_id in current_event_ids
        }
        for index, event_type in enumerate(self.event_types):
            if event_type == "user.message":
                protected.add(index)
        complete = [group for group in groups if not group["pending"]]
        failures = [
            group
            for group in complete
            if any(item.get("status") not in {"succeeded", "rejected"} for item in group["results"])
        ]
        recent_complete = complete[-1] if complete else None
        recent_failure = failures[-1] if failures else None
        candidates: list[dict[str, Any]] = []
        for group in groups:
            indices = group["indices"]
            if group["pending"]:
                plan["pending_tool_interactions_kept"] += 1
                protected.update(indices)
            elif group is recent_complete or group is recent_failure or indices & protected:
                protected.update(indices)
            else:
                candidates.append(group)

        selected: list[dict[str, Any]] = []
        removed_total = 0
        self._sync_sizes()
        # One model summary replaces the whole prefix, so a single short group
        # must not be dropped just because a per-group template would not shrink.
        for group in candidates:
            indices = group["indices"]
            removed_chars = sum(self._sizes[index] for index in indices)
            if removed_chars <= 0:
                continue
            selected.append(group)
            removed_total += removed_chars
            if before - removed_total + SUMMARY_RESERVE <= limit:
                break
        if removed_total <= SUMMARY_RESERVE:
            selected = []
            removed_total = 0

        remove: set[int] = set()
        for group in selected:
            remove.update(group["indices"])
        estimated = before - removed_total + (SUMMARY_RESERVE if selected else 0)
        if estimated > limit:
            for indices, _summary in self._plain_turns(current_event_ids):
                if estimated <= limit:
                    break
                removable = [
                    index
                    for index in indices
                    if self.event_types[index] != "user.message" and index not in protected
                ]
                removed_chars = sum(self._sizes[index] for index in removable)
                if removed_chars <= 0:
                    continue
                remove.update(removable)
                estimated -= removed_chars
                plan["plain_turns"] += 1

        if not remove:
            return plan
        ordered = sorted(remove)
        plan["remove_indices"] = ordered
        plan["insert_at"] = ordered[0]
        plan["summary_input"] = [self.messages[index] for index in ordered]
        plan["completed_tool_interactions"] = len(selected)
        return plan

    def apply_model_summary(self, plan: dict[str, Any], summary_text: str) -> dict[str, Any]:
        """Replace a planned prefix. Failure leaves the record untouched."""
        before = self.serialized_length()
        kept = {
            "status": "kept_original",
            "source": "model",
            "chars_before": before,
            "chars_after": before,
            "chars_recovered": 0,
            "completed_tool_interactions": 0,
            "plain_turns": 0,
            "pending_tool_interactions_kept": plan.get("pending_tool_interactions_kept", 0),
        }
        indices = {int(index) for index in plan.get("remove_indices", [])}
        insert_at = plan.get("insert_at")
        text = summary_text.strip()
        if not indices or not isinstance(insert_at, int) or not text:
            return kept
        if any(index < 0 or index >= len(self.messages) for index in indices):
            return kept
        summary = _model_summary_message(text)
        self._sync_sizes()
        removed_chars = sum(self._sizes[index] for index in indices)
        if len(json.dumps(summary, ensure_ascii=False)) >= removed_chars:
            return kept
        self.archive.extend(self.messages[index] for index in sorted(indices))
        messages: list[dict[str, Any]] = []
        sizes: list[int] = []
        event_ids: list[str | None] = []
        event_types: list[str | None] = []
        for index, message in enumerate(self.messages):
            if index == insert_at:
                messages.append(summary)
                sizes.append(self._size_of(summary))
                event_ids.append(None)
                event_types.append(None)
            if index in indices:
                continue
            messages.append(message)
            sizes.append(self._sizes[index])
            event_ids.append(self.event_ids[index])
            event_types.append(self.event_types[index])
        self.messages = messages
        self._sizes = sizes
        self._size_sum = sum(sizes)
        self.event_ids = event_ids
        self.event_types = event_types
        self._signature = (len(messages), len(event_ids))
        self.version += 1
        after = self.serialized_length()
        return {
            "status": "applied",
            "source": "model",
            "chars_before": before,
            "chars_after": after,
            "chars_recovered": before - after,
            "completed_tool_interactions": plan.get("completed_tool_interactions", 0),
            "plain_turns": plan.get("plain_turns", 0),
            "pending_tool_interactions_kept": plan.get("pending_tool_interactions_kept", 0),
        }

    def _append(
        self,
        message: dict[str, Any],
        event_id: str | None = None,
        event_type: str | None = None,
    ) -> int:
        self._sync_sizes()
        size = self._size_of(message)
        self.messages.append(message)
        self.event_ids.append(event_id)
        self.event_types.append(event_type)
        self._sizes.append(size)
        self._size_sum += size
        self._signature = (len(self.messages), len(self.event_ids))
        self.version += 1
        # The system message guarantees one existing list item, so each append
        # adds the default JSON separator ", " plus the serialized message.
        return size + self._SEPARATOR_CHARS

    def _runtime_event(self, index: int) -> dict[str, Any] | None:
        message = self.messages[index]
        content = message.get("content")
        if message.get("role") != "user" or not isinstance(content, str):
            return None
        try:
            envelope = json.loads(content)
        except (TypeError, json.JSONDecodeError):
            return None
        if not isinstance(envelope, dict) or envelope.get("kind") != "runtime_event":
            return None
        return envelope

    def _tool_interactions(self) -> list[dict[str, Any]]:
        receipts: dict[str, int] = {}
        finished: dict[str, int] = {}
        for index, message in enumerate(self.messages):
            if message.get("role") == "tool" and isinstance(message.get("tool_call_id"), str):
                receipts[str(message["tool_call_id"])] = index
            envelope = self._runtime_event(index)
            if envelope and envelope.get("type") == "python.finished":
                reply_to = envelope.get("reply_to")
                if isinstance(reply_to, str):
                    finished[reply_to] = index

        groups: list[dict[str, Any]] = []
        for index, message in enumerate(self.messages):
            calls = message.get("tool_calls")
            if message.get("role") != "assistant" or not isinstance(calls, list) or not calls:
                continue
            indices = {index}
            receipt_values: list[dict[str, Any]] = []
            pending = False
            for call in calls:
                call_id = call.get("id") if isinstance(call, dict) else None
                receipt_index = receipts.get(call_id) if isinstance(call_id, str) else None
                if receipt_index is None:
                    pending = True
                    continue
                indices.add(receipt_index)
                try:
                    receipt = json.loads(str(self.messages[receipt_index].get("content") or "{}"))
                except json.JSONDecodeError:
                    receipt = {}
                if not isinstance(receipt, dict):
                    receipt = {}
                receipt_values.append(receipt)
                if receipt.get("status") == "accepted":
                    execution_id = receipt.get("execution_id")
                    if not isinstance(execution_id, str):
                        pending = True
                        continue
                    result_index = finished.get(execution_id)
                    if result_index is None:
                        pending = True
                    else:
                        indices.add(result_index)
                elif receipt.get("status") != "rejected":
                    pending = True
            results = []
            for result_index in sorted(indices):
                envelope = self._runtime_event(result_index)
                if envelope and envelope.get("type") == "python.finished":
                    payload = envelope.get("payload")
                    if isinstance(payload, dict):
                        results.append(
                            {
                                "execution_id": envelope.get("reply_to"),
                                "status": payload.get("status"),
                                "duration_ms": payload.get("duration_ms"),
                                "stdout_bytes": payload.get("stdout_bytes"),
                                "stderr_bytes": payload.get("stderr_bytes"),
                                "stdout": payload.get("stdout"),
                                "stderr": payload.get("stderr"),
                                "stdout_artifact_path": payload.get("stdout_artifact_path"),
                                "stderr_artifact_path": payload.get("stderr_artifact_path"),
                            }
                        )
            groups.append(
                {
                    "indices": indices,
                    "receipts": receipt_values,
                    "results": results,
                    "pending": pending,
                }
            )
        return groups

    def _protected_indices(self, current_event_ids: set[str]) -> set[int]:
        protected = {
            index
            for index, event_id in enumerate(self.event_ids)
            if event_id is not None and event_id in current_event_ids
        }
        user_indices = [
            index
            for index, event_type in enumerate(self.event_types)
            if event_type == "user.message"
        ]
        if user_indices:
            protected.add(user_indices[0])
            protected.add(user_indices[-1])
        protected.update(range(max(0, len(self.messages) - 8), len(self.messages)))
        return protected

    def _plain_turns(self, current_event_ids: set[str]) -> list[tuple[list[int], dict[str, Any]]]:
        protected = self._protected_indices(current_event_ids)
        starts = [
            index
            for index, event_type in enumerate(self.event_types)
            if event_type == "user.message"
        ]
        turns: list[tuple[list[int], dict[str, Any]]] = []
        for position, start in enumerate(starts):
            end = starts[position + 1] if position + 1 < len(starts) else len(self.messages)
            indices = list(range(start, end))
            if start in protected or any(index in protected for index in indices):
                continue
            if any(
                self.messages[index].get("role") not in {"user", "assistant"}
                or self.messages[index].get("tool_calls")
                or self.event_types[index] not in {None, "user.message"}
                or self._runtime_event(index) is not None
                for index in indices
            ):
                continue
            user_text = str(self.messages[start].get("content") or "")
            assistant_text = " ".join(
                str(self.messages[index].get("content") or "").strip()
                for index in indices
                if self.messages[index].get("role") == "assistant"
            ).strip()
            summary = (
                "[Earlier completed conversation compacted; original events remain in the journal. "
                f"User: {self._excerpt(user_text, 100)} "
                f"Assistant: {self._excerpt(assistant_text, 100)}]"
            )
            turns.append((indices, {"role": "user", "content": summary}))
        return turns

    def _tool_summary(self, group: dict[str, Any]) -> dict[str, Any]:
        lines = [
            "[Earlier completed Python interaction compacted. It already ran; "
            "do not replay it automatically. Raw events and full output artifacts remain available.]"
        ]
        for result in group["results"]:
            execution_id = result.get("execution_id")
            status = result.get("status") or "unknown"
            lines.append(
                f"execution={execution_id} status={status} duration_ms={result.get('duration_ms')} "
                f"stdout_bytes={result.get('stdout_bytes')} stderr_bytes={result.get('stderr_bytes')}"
            )
            for stream in ("stdout", "stderr"):
                excerpt = result.get(stream)
                if isinstance(excerpt, str) and excerpt.strip():
                    lines.append(f"{stream}: {self._excerpt(excerpt, 120)}")
                artifact = result.get(f"{stream}_artifact_path")
                if isinstance(artifact, str):
                    lines.append(f"{stream} full output: {artifact}")
        for receipt in group["receipts"]:
            if receipt.get("status") == "rejected":
                lines.append(f"tool call rejected: {receipt.get('reason') or 'rejected'}")
        return {"role": "user", "content": "\n".join(lines)}

    @staticmethod
    def _excerpt(value: str, limit: int) -> str:
        value = " ".join(value.split())
        if len(value) <= limit:
            return value
        head = max(1, limit * 2 // 3)
        tail = max(1, limit - head - 1)
        return f"{value[:head]}…{value[-tail:]}"

    def _replace_indices(
        self,
        indices: list[int],
        insert_at: int,
        summary: dict[str, Any],
    ) -> None:
        for index in reversed(indices):
            self.messages.pop(index)
            self.event_ids.pop(index)
            self.event_types.pop(index)
            self._size_sum -= self._sizes.pop(index)
        summary_size = self._size_of(summary)
        self.messages.insert(insert_at, summary)
        self.event_ids.insert(insert_at, None)
        self.event_types.insert(insert_at, None)
        self._sizes.insert(insert_at, summary_size)
        self._size_sum += summary_size
        self._signature = (len(self.messages), len(self.event_ids))


def _model_summary_message(text: str) -> dict[str, Any]:
    body = (
        "[Earlier interactions summarized for context. This is not runtime status and "
        "must not be treated as the current execution result. Completed code already ran; "
        "do not replay it automatically. Original execution records remain available.]\n"
        + text.strip()
    )
    return {"role": "user", "content": body}


class ContextProjector:
    """Replaceable model view. The bus only delivers; this decides what is seen."""

    def __init__(
        self,
        transcript: Transcript,
        runtime_facts: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self.transcript = transcript
        self.runtime_facts = runtime_facts or (lambda: {})

    def status_message(self) -> dict[str, Any] | None:
        facts = self.runtime_facts() or {}
        if not facts:
            return None
        envelope = {
            "kind": "runtime_status",
            "source": "runtime",
            "note": (
                "Live Runtime observation. A context summary must not override "
                "worker epoch, unfinished executions, or namespace validity."
            ),
            "worker_epoch": facts.get("worker_epoch"),
            "namespace": facts.get("namespace"),
            "current_execution_id": facts.get("current_execution_id"),
            "queued_execution_ids": list(facts.get("queued_execution_ids") or []),
            "unfinished_execution_ids": list(facts.get("unfinished_execution_ids") or []),
        }
        return {
            "role": "user",
            "content": json.dumps(envelope, ensure_ascii=False, separators=(",", ":")),
        }

    def snapshot(self) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        status = self.status_message()
        if status is not None:
            messages.append(status)
        messages.extend(self.transcript.messages)
        return messages

    def view_length(self) -> int:
        """Serialized size of the model view, without dumping the history.

        Every list item is serialized independently and joined by ", ", so the
        view adds the status item's own size plus one separator to the
        transcript's cached total.
        """
        return self.transcript.serialized_length() + self.status_overhead()

    def status_overhead(self) -> int:
        status = self.status_message()
        if status is None:
            return 0
        return len(json.dumps(status, ensure_ascii=False)) + 2

    def minimum_floor(self) -> int:
        return self.transcript._base_chars + self.status_overhead()

    def diagnose(self, *, context_limit: int) -> str | None:
        ids = [event_id for event_id in self.transcript.event_ids if event_id]
        if len(ids) != len(set(ids)):
            return "duplicate_append"
        if context_limit < self.minimum_floor():
            return "budget_config"
        return None


class LLMActor:
    def __init__(
        self,
        *,
        bus_state: Callable[[], RuntimeState],
        bus_seq: Callable[[], int],
        inbox: Mailbox,
        backend: Any,
        emitter: Emitter,
        ui: Emitter,
        workspace: str,
        model: str,
        session_id: str,
        journal: Any | None = None,
        max_activations: int = 100,
        max_timeout: float = 120,
        default_timeout: float = 30,
        context_limit: int = 200_000,
        batch_limit: int = 32,
        runtime_facts: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._bus_state = bus_state
        self._bus_seq = bus_seq
        self.inbox = inbox
        self.backend = backend
        self._emitter = emitter
        self._ui = ui
        self.workspace = workspace
        self.model = model
        self.session_id = session_id
        self._journal = journal
        self.max_activations = max_activations
        self.max_timeout = max_timeout
        self.default_timeout = min(default_timeout, max_timeout)
        self.context_limit = context_limit
        self.batch_limit = batch_limit
        self.transcript = Transcript()
        self.projector = ContextProjector(self.transcript, runtime_facts)
        self.compacting = False
        self.note: Callable[[str], None] | None = None
        self._last_request_chars: int | None = None
        self.tools = [tool_schema(workspace), environment_tool_schema()]
        self.activations: list[Activation] = []
        self._paused: Activation | None = None
        self._held: list[Event] = []
        self._stop = False
        self._between_user_calls = 0
        self.phase = "stopped"
        self.busy = False
        self._inflight: asyncio.Task[Any] | None = None
        self.after_tool: Callable[[int, ToolCall], None] | None = None
        self._stopped: asyncio.Event | None = None

    @property
    def paused(self) -> Activation | None:
        return self._paused

    @property
    def held(self) -> list[Event]:
        return self._held

    def request_stop(self) -> None:
        self._stop = True
        inflight = self._inflight
        if inflight is not None and not inflight.done():
            inflight.cancel()

    async def run(self) -> None:
        self._stopped = asyncio.Event()
        try:
            while not self._stop:
                if self._paused is not None:
                    self.phase = "waiting"
                    batch = await self.inbox.take_batch(self.batch_limit)
                    self.phase = "working"
                    if not batch or self._stop:
                        break
                    if not self._resume_allowed(batch):
                        self._held.extend(batch)
                        continue
                    kinds = {event.type for event in batch}
                    if self._paused.error_kind == "context" and "agent.retry" in kinds:
                        remaining = [event for event in batch if event.type != "agent.retry"]
                        for event in batch:
                            if event.type == "agent.retry":
                                self.inbox.mark_consumed(event)
                        self._held.extend(remaining)
                        if "user.message" in kinds:
                            self._between_user_calls = 0
                            self._paused = None
                        else:
                            self._ui.call(
                                "agent.error",
                                {
                                    "kind": "context",
                                    "message": "Retry cannot reduce an oversized transcript; submit a new or shorter message. No provider request was sent.",
                                },
                            )
                        continue
                    if self._paused.error_kind in {"budget", "context"}:
                        self._between_user_calls = 0
                        self._paused = None
                        self._held.extend(batch)
                        continue
                    self._held.extend(batch)
                    await self._retry(self._paused)
                    continue
                if self._held:
                    pending = self._held
                    self._held = []
                    extra = self.inbox.drain_available(max(0, self.batch_limit - len(pending)))
                    combined = pending + extra
                    batch = combined[: self.batch_limit]
                    self._held = combined[self.batch_limit :]
                else:
                    self.phase = "waiting"
                    batch = await self.inbox.take_batch(self.batch_limit)
                    self.phase = "working"
                    if not batch:
                        break
                if self._stop or self._bus_state() is not RuntimeState.RUNNING:
                    break
                batch = await self._coalesce_handler_results(batch)
                await self._activate(batch)
        finally:
            self.phase = "stopped"
            self.busy = False
            if self._stopped is not None:
                self._stopped.set()

    async def wait_stopped(self) -> None:
        if self._stopped is None:
            return
        await self._stopped.wait()

    async def _coalesce_handler_results(self, batch: list[Event]) -> list[Event]:
        """Wait for the handler frames announced by each finished request.

        Older finished events have no count, so retain their bounded 25 ms grace.
        """
        finished = {
            event.reply_to: event
            for event in batch
            if event.type == "python.finished" and isinstance(event.reply_to, str)
        }
        if not finished:
            return batch
        expected: dict[str, int | None] = {}
        received: dict[str, set[str]] = {}

        def register_finished(events: list[Event]) -> None:
            for event in events:
                if event.type != "python.finished" or not isinstance(event.reply_to, str):
                    continue
                request_id = event.reply_to
                expected[request_id] = event.payload.get("expected_handler_count")
                received.setdefault(request_id, set())

        register_finished(batch)
        deadline = asyncio.get_running_loop().time() + 0.025
        found: list[Event] = []

        def count_handlers(events: list[Event]) -> None:
            for event in events:
                if event.type == "agent.handler_fired":
                    trigger = event.payload.get("trigger")
                    request_id = trigger.get("request_id") if isinstance(trigger, dict) else None
                    if request_id in received:
                        handler_id = event.payload.get("handler_id")
                        # Host events always carry a handler ID. Use the event ID
                        # for malformed legacy producers so separate observations
                        # still count independently while duplicate identified
                        # results cannot satisfy the expected count twice.
                        received[request_id].add(
                            handler_id if isinstance(handler_id, str) else event.id
                        )

        count_handlers(batch)
        while True:
            all_counted_complete = all(
                count is None or len(received[request_id]) >= count
                for request_id, count in expected.items()
            )
            legacy_pending = any(count is None for count in expected.values())
            now = asyncio.get_running_loop().time()
            if all_counted_complete and (not legacy_pending or now >= deadline):
                break
            remaining = deadline - now
            if remaining <= 0:
                break
            try:
                extra = await asyncio.wait_for(
                    self.inbox.take_batch(self.batch_limit), remaining
                )
            except asyncio.TimeoutError:
                break
            if not extra:
                break
            found.extend(extra)
            register_finished(extra)
            count_handlers(extra)
        return sorted([*batch, *found], key=lambda event: event.seq)

    def _resume_allowed(self, batch: list[Event]) -> bool:
        kinds = {event.type for event in batch}
        if self._paused is None:
            return False
        if "agent.retry" in kinds:
            return True
        if self._paused.error_kind in {"response_unknown", "commit_interrupted"}:
            return False
        if self._paused.error_kind == "budget":
            return "user.message" in kinds
        return "user.message" in kinds

    async def _activate(self, batch: list[Event]) -> None:
        self.busy = True
        try:
            # Summarize the existing view first so this batch, and anything that
            # arrives while the summary is in flight, is consumed afterwards.
            pre_compaction = await self._maybe_compress(current_event_ids=set())
            activation = Activation(
                id=f"act_{uuid.uuid4().hex[:12]}",
                input_event_ids=tuple(event.id for event in batch),
                transcript_version=self.transcript.version,
                status="running",
                input_high_water_seq=max(event.seq for event in batch),
                started_at=time.time(),
            )
            self.activations.append(activation)
            version_before = self.transcript.version
            before_chars = self.transcript.serialized_length()
            event_metrics: list[dict[str, Any]] = []
            input_added_chars = 0
            for event in batch:
                projected = self.transcript.project(event)
                self.inbox.mark_consumed(event)
                added_chars = int(projected["added_chars"])
                input_added_chars += added_chars
                detail: dict[str, Any] = {
                    "id": event.id,
                    "type": event.type,
                    "projected": projected["projected"],
                    "duplicate": projected["duplicate"],
                    "added_chars": added_chars,
                }
                if event.type == "python.finished":
                    detail["output_preview_chars"] = sum(
                        len(str(event.payload.get(stream) or ""))
                        for stream in ("stdout", "stderr")
                    )
                    detail["stdout_bytes"] = event.payload.get("stdout_bytes")
                    detail["stderr_bytes"] = event.payload.get("stderr_bytes")
                    detail["truncated"] = bool(event.payload.get("truncated"))
                event_metrics.append(detail)
            record_after = self.transcript.serialized_length()
            accounting_mismatch = record_after - before_chars != input_added_chars
            if (
                not accounting_mismatch
                and self.projector.view_length() > self.context_limit - SUMMARY_RESERVE
            ):
                post_compaction = await self._maybe_compress(
                    current_event_ids=set(activation.input_event_ids)
                )
            elif accounting_mismatch and self.projector.view_length() > self.context_limit:
                post_compaction = {
                    "status": "skipped",
                    "reason": "duplicate_append",
                    "source": "model",
                    "chars_recovered": 0,
                    "completed_tool_interactions": 0,
                    "plain_turns": 0,
                    "pending_tool_interactions_kept": 0,
                }
            else:
                post_compaction = None
            compaction = post_compaction if post_compaction is not None else pre_compaction
            view_chars = self.projector.view_length()
            activation.context_metrics = {
                "measurement": "serialized transcript JSON characters; not tokenizer tokens",
                "configured_limit_chars": self.context_limit,
                "transcript_chars_before": before_chars,
                "input_added_chars": input_added_chars,
                "transcript_chars_after_inputs": record_after,
                "previous_request_chars": self._last_request_chars,
                "growth_since_previous_request_chars": (
                    None
                    if self._last_request_chars is None
                    else view_chars - self._last_request_chars
                ),
                "tool_schema_chars": len(json.dumps(self.tools, ensure_ascii=False)),
                "input_events": event_metrics,
                "attempts": [],
                "compaction": compaction,
            }
            activation.transcript_version = self.transcript.version
            if self.transcript.version == version_before and compaction is None:
                self._journal_activation(activation)
                self._finish(activation, "committed")
                return

            if compaction is not None:
                activation.context_metrics["transcript_chars_after_compaction"] = view_chars
            if view_chars > self.context_limit:
                recovered = int((compaction or {}).get("chars_recovered") or 0)
                reason = str((compaction or {}).get("reason") or (compaction or {}).get("status") or "")
                usage = self._latest_provider_usage()
                usage_text = (
                    json.dumps(usage, ensure_ascii=False, separators=(",", ":"))
                    if usage is not None
                    else "unavailable"
                )
                prefix = ""
                if reason == "budget_config":
                    prefix = (
                        "Configured context budget is below the fixed prompt and live runtime status, "
                        "so history was not compressed to hide that. "
                    )
                elif reason == "duplicate_append":
                    prefix = (
                        "Context growth did not match one append of this batch, "
                        "so history was not compressed to hide a duplicate append. "
                    )
                if reason == "summary_failed":
                    request_note = "The summary request failed and the original context was kept. The task request was not sent."
                else:
                    request_note = "No provider request was sent."
                message = (
                    f"{prefix}"
                    f"serialized transcript estimate {view_chars:,} chars exceeds configured "
                    f"limit {self.context_limit:,} chars; this activation added {input_added_chars:,} "
                    f"chars before compaction, which recovered {recovered:,}. "
                    f"{request_note} The estimate is JSON characters, not tokens, "
                    "and excludes tool-schema/provider framing. Latest provider-reported usage "
                    f"(provider-defined units): {usage_text}."
                )
                self._journal_activation(activation)
                await self._pause(activation, "context", message)
                return

            activation.context_metrics["request_transcript_chars"] = view_chars
            activation.transcript_version = self.transcript.version
            self._journal_activation(activation)
            has_user = any(event.type == "user.message" for event in batch)
            if not has_user and self._between_user_calls >= self.max_activations:
                await self._pause(
                    activation,
                    "budget",
                    f"activation budget of {self.max_activations} reached between user messages",
                )
                return
            if has_user:
                self._between_user_calls = 0
            activation.snapshot = self.projector.snapshot()
            self._between_user_calls += 1
            await self._complete_and_commit(activation)
        finally:
            self.busy = False

    async def _maybe_compress(self, *, current_event_ids: set[str]) -> dict[str, Any] | None:
        view = self.projector.view_length()
        if view <= self.context_limit - SUMMARY_RESERVE:
            return None
        diagnosis = self.projector.diagnose(context_limit=self.context_limit)
        if diagnosis:
            return {
                "status": "skipped",
                "reason": diagnosis,
                "source": "model",
                "chars_before": view,
                "chars_after": view,
                "chars_recovered": 0,
                "completed_tool_interactions": 0,
                "plain_turns": 0,
                "pending_tool_interactions_kept": 0,
            }
        overhead = self.projector.status_overhead()
        floor = len(json.dumps([{"role": "system", "content": SYSTEM_PROMPT}], ensure_ascii=False))
        target = self.context_limit - overhead - SUMMARY_RESERVE
        if target < floor:
            return {
                "status": "skipped",
                "reason": "budget_config",
                "source": "model",
                "chars_before": view,
                "chars_after": view,
                "chars_recovered": 0,
                "completed_tool_interactions": 0,
                "plain_turns": 0,
                "pending_tool_interactions_kept": 0,
            }
        plan = self.transcript.plan_compaction(current_event_ids=current_event_ids, limit=target)
        if not plan["remove_indices"] and view > self.context_limit:
            plan = self.transcript.plan_compaction(
                current_event_ids=current_event_ids,
                limit=max(floor, self.context_limit - overhead),
            )
        if not plan["remove_indices"]:
            if view > self.context_limit:
                return {
                    "status": "skipped",
                    "reason": "nothing_compactable",
                    "source": "model",
                    "chars_before": view,
                    "chars_after": view,
                    "chars_recovered": 0,
                    "completed_tool_interactions": 0,
                    "plain_turns": 0,
                    "pending_tool_interactions_kept": plan.get("pending_tool_interactions_kept", 0),
                }
            return None
        self.compacting = True
        self._note(f"context  compressing  {view:,}/{self.context_limit:,}")
        try:
            text = await self._request_summary(plan["summary_input"])
        finally:
            self.compacting = False
        after_view = self.projector.view_length()
        if not text:
            self._note(f"context  kept original  {after_view:,}/{self.context_limit:,}")
            return {
                "status": "kept_original",
                "reason": "summary_failed",
                "source": "model",
                "chars_before": view,
                "chars_after": after_view,
                "chars_recovered": 0,
                "completed_tool_interactions": 0,
                "plain_turns": 0,
                "pending_tool_interactions_kept": plan.get("pending_tool_interactions_kept", 0),
            }
        applied = self.transcript.apply_model_summary(plan, text)
        after_view = self.projector.view_length()
        if applied["status"] == "applied":
            self._note(
                f"context  {after_view:,}/{self.context_limit:,}  recovered {applied['chars_recovered']:,}"
            )
        else:
            self._note(f"context  kept original  {after_view:,}/{self.context_limit:,}")
        return applied

    async def _request_summary(self, summary_input: list[dict[str, Any]]) -> str | None:
        request = ModelRequest(
            messages=[
                {"role": "system", "content": SUMMARY_INSTRUCTION},
                {"role": "user", "content": json.dumps(summary_input, ensure_ascii=False)},
            ],
            tools=[],
            activation_id=f"sum_{uuid.uuid4().hex[:12]}",
            model=self.model,
            purpose="context_summary",
        )
        self._inflight = asyncio.create_task(self.backend.complete(request))
        try:
            response = await self._inflight
        except asyncio.CancelledError:
            raise
        except (ProviderError, Exception):
            return None
        finally:
            self._inflight = None
        if not isinstance(response, ModelResponse) or response.tool_calls:
            return None
        text = (response.content or "").strip()
        if not text or len(text) > SUMMARY_RESERVE:
            return None
        return text

    def _note(self, text: str) -> None:
        note = self.note
        if note is not None:
            note(text)

    async def _retry(self, activation: Activation) -> None:
        self.busy = True
        try:
            if activation.error_kind == "commit_interrupted" and activation.response is not None:
                activation.status = "running"
                self._deliver_tools(activation, activation.response)
                if activation.status == "running":
                    self._finish(activation, "committed")
                return
            if activation.error_kind == "budget":
                self._between_user_calls = max(0, self.max_activations - 1)
            self._between_user_calls += 1
            await self._complete_and_commit(activation)
        finally:
            self.busy = False

    async def _complete_and_commit(self, activation: Activation) -> None:
        activation.status = "running"
        if activation.snapshot is None:
            activation.snapshot = self.projector.snapshot()
        request = ModelRequest(
            messages=activation.snapshot,
            tools=self.tools,
            activation_id=activation.id,
            model=self.model,
        )
        request_chars = len(json.dumps(request.messages, ensure_ascii=False))
        tools_chars = len(json.dumps(request.tools, ensure_ascii=False))
        attempts = activation.context_metrics.setdefault("attempts", [])
        attempt: dict[str, Any] = {
            "number": len(attempts) + 1,
            "transcript_chars": request_chars,
            "tool_schema_chars": tools_chars,
            "configured_limit_chars": self.context_limit,
            "started_at": time.time(),
            "status": "running",
        }
        attempts.append(attempt)
        activation.context_metrics["request_transcript_chars"] = request_chars
        activation.context_metrics["tool_schema_chars"] = tools_chars
        self._last_request_chars = request_chars
        self._journal_activation(activation)
        attempt_started = time.monotonic()

        def finish_attempt(
            status: str,
            *,
            response: ModelResponse | None = None,
            error: str | None = None,
        ) -> None:
            attempt["status"] = status
            attempt["duration_ms"] = max(0, int((time.monotonic() - attempt_started) * 1000))
            if response is not None:
                attempt["model"] = response.model or self.model
                attempt["usage"] = response.usage
                activation.context_metrics["provider_usage"] = response.usage
                activation.context_metrics["provider_model"] = response.model or self.model
            if error:
                attempt["error"] = error

        self._inflight = asyncio.create_task(self.backend.complete(request))
        try:
            response = await self._inflight
        except asyncio.CancelledError:
            finish_attempt("cancelled")
            if self._stop:
                activation.status = "failed"
                activation.error_kind = "shutdown"
                self._journal_activation(activation)
                return
            raise
        except ProviderError as exc:
            finish_attempt(exc.kind, error=exc.message)
            await self._pause(activation, exc.kind, exc.message)
            return
        except Exception as exc:
            message = f"provider response unknown: {exc}"
            finish_attempt("response_unknown", error=message)
            await self._pause(activation, "response_unknown", message)
            return
        finally:
            self._inflight = None
        if not isinstance(response, ModelResponse):
            finish_attempt("response_invalid", error="backend returned a non-response")
            await self._pause(activation, "response_invalid", "backend returned a non-response")
            return
        finish_attempt("response_received", response=response)
        problems = self._validate(response)
        if problems:
            if problems == ["empty"]:
                await self._pause(activation, "response_invalid", "model response was empty")
                return
            self._reject_all(activation, response, problems)
            return
        activation.response = response
        if not activation.response_recorded:
            self.transcript.add_assistant(response)
            activation.response_recorded = True
            if response.content and response.content.strip():
                self._ui.call(
                    "assistant.message",
                    {"text": response.content, "activation_id": activation.id},
                )
        self._deliver_tools(activation, response)
        if activation.status == "running":
            self._finish(activation, "committed")

    def _validate(self, response: ModelResponse) -> list[str]:
        if not response.tool_calls and not (response.content and response.content.strip()):
            return ["empty"]
        errors: list[str] = []
        seen: set[str] = set()
        for call in response.tool_calls:
            if call.invalid:
                errors.append(f"{call.id}:{call.invalid}")
                continue
            if not call.id or call.id in seen:
                errors.append(f"{call.id}:invalid_payload")
                continue
            seen.add(call.id)
            if call.name == "python_environment":
                arguments = call.arguments or {}
                modules = arguments.get("modules", [])
                api_names = arguments.get("api_names", [])
                if (
                    not isinstance(modules, list)
                    or any(not isinstance(item, str) for item in modules)
                    or not isinstance(api_names, list)
                    or any(not isinstance(item, str) for item in api_names)
                ):
                    errors.append(f"{call.id}:invalid_payload")
                continue
            if call.name != "python_exec":
                errors.append(f"{call.id}:unknown_tool")
                continue
            arguments = call.arguments or {}
            code = arguments.get("code")
            if not isinstance(code, str):
                errors.append(f"{call.id}:invalid_payload")
                continue
            if "timeout" in arguments and arguments["timeout"] is not None:
                timeout = arguments["timeout"]
                if (
                    isinstance(timeout, bool)
                    or not isinstance(timeout, (int, float))
                    or not math.isfinite(float(timeout))
                    or float(timeout) <= 0
                    or float(timeout) > self.max_timeout
                ):
                    errors.append(f"{call.id}:invalid_timeout")
        return errors

    def _reject_all(self, activation: Activation, response: ModelResponse, problems: list[str]) -> None:
        if not activation.response_recorded:
            self.transcript.add_assistant(response)
            activation.response_recorded = True
        reasons = {item.split(":", 1)[0]: item.split(":", 1)[-1] for item in problems}
        rejections = []
        for call in response.tool_calls:
            if activation.tool_calls.get(call.id) in {"accepted", "rejected"}:
                continue
            reason = reasons.get(call.id, "invalid_payload")
            receipt = {"status": "rejected", "reason": "invalid_payload", "executed": False, "detail": reason}
            self.transcript.add_tool(call.id, receipt)
            activation.tool_calls[call.id] = "rejected"
            rejections.append({"tool_call_id": call.id, "reason": reason})
            self._journal_tool(activation, call.id, None, "rejected", reason)
        self._emit_feedback(activation, rejections or [{"tool_call_id": "", "reason": "invalid_payload"}])
        self._finish(activation, "committed")

    def _deliver_tools(self, activation: Activation, response: ModelResponse) -> None:
        rejections: list[dict[str, str]] = []
        for index, call in enumerate(response.tool_calls):
            state = activation.tool_calls.get(call.id)
            if state in {"accepted", "rejected"}:
                continue
            activation.tool_calls[call.id] = "pending"
            try:
                receipt, status = self._deliver_one(activation, call)
                self.transcript.add_tool(call.id, receipt)
                activation.tool_calls[call.id] = status
                execution_id = receipt.get("execution_id") if status == "accepted" else None
                reason = receipt.get("reason") if status == "rejected" else None
                self._journal_tool(
                    activation,
                    call.id,
                    execution_id if isinstance(execution_id, str) else None,
                    status,
                    reason if isinstance(reason, str) else None,
                )
                if status == "rejected":
                    rejections.append(
                        {"tool_call_id": call.id, "reason": str(receipt.get("reason") or "rejected")}
                    )
                if self.after_tool is not None:
                    self.after_tool(index, call)
            except Exception as exc:
                activation.error_kind = "commit_interrupted"
                activation.error_message = f"commit interrupted: {exc}"
                self._pause_inplace(activation)
                return
        if rejections and not activation.feedback_sent:
            self._emit_feedback(activation, rejections)

    def _deliver_one(self, activation: Activation, call: ToolCall) -> tuple[dict[str, Any], str]:
        arguments = call.arguments or {}
        if call.name == "python_environment":
            try:
                if not isinstance(arguments, dict) or set(arguments) - {"modules", "api_names"}:
                    raise ValueError("invalid environment discovery arguments")
                result = discover_python_environment(
                    self.workspace,
                    arguments.get("modules", []),
                    arguments.get("api_names", []),
                )
                result["status"] = "succeeded"
            except Exception as exc:
                result = {
                    "status": "failed",
                    "error": f"environment discovery failed ({type(exc).__name__})"[:500],
                    "read_only": True,
                    "python": sys.version.split()[0],
                    "workspace": self.workspace,
                    "project_files": [],
                    "modules": [],
                    "install_supported": False,
                }
            delivery = self._emitter.call(
                "python.environment_discovered",
                {"activation_id": activation.id, "tool_call_id": call.id, **result},
                reply_to=activation.id,
            )
            if not delivery.accepted:
                return {"status": "rejected", "reason": delivery.reason or "rejected", "executed": False}, "rejected"
            return {"status": "accepted", "event_id": delivery.event_id}, "accepted"
        timeout = arguments.get("timeout", self.default_timeout)
        delivery = self._emitter.call(
            "python.requested",
            {
                "code": arguments["code"],
                "timeout": float(timeout),
                "activation_id": activation.id,
                "tool_call_id": call.id,
            },
            reserve_result_for="llm",
            reserve_handler_slots=MAX_HANDLER_RESULTS_PER_EXECUTION,
        )
        if not delivery.accepted:
            return {"status": "rejected", "reason": delivery.reason or "rejected", "executed": False}, "rejected"
        return {"status": "accepted", "execution_id": delivery.event_id}, "accepted"

    def _emit_feedback(self, activation: Activation, rejections: list[dict[str, str]]) -> None:
        delivery = self._emitter.call(
            "agent.feedback",
            {"activation_id": activation.id, "rejections": rejections},
            lane=Lane.FEEDBACK,
            lane_key=activation.id,
        )
        activation.feedback_sent = delivery.accepted
        if not delivery.accepted:
            self._ui.call(
                "agent.error",
                {
                    "kind": "feedback_undelivered",
                    "message": f"correction feedback for {activation.id} was not delivered: {delivery.reason}",
                },
            )

    async def _pause(self, activation: Activation, kind: str, message: str) -> None:
        activation.error_kind = kind
        activation.error_message = message
        self._pause_inplace(activation)
        self._journal_activation(activation)

    def _pause_inplace(self, activation: Activation) -> None:
        activation.status = "paused"
        self._paused = activation
        kind = activation.error_kind or "paused"
        message = activation.error_message or kind
        if kind == "response_unknown":
            message = f"{message}. Choose /retry, abandon, or check the provider; this request will not be repeated automatically."
        self._ui.call("agent.error", {"kind": kind, "message": message, "activation_id": activation.id})

    def _finish(self, activation: Activation, status: str) -> None:
        activation.status = status
        activation.ended_at = time.time()
        if activation.context_metrics:
            after_commit = self.transcript.serialized_length()
            activation.context_metrics["transcript_chars_after_commit"] = after_commit
            request_chars = activation.context_metrics.get("request_transcript_chars")
            if isinstance(request_chars, int):
                activation.context_metrics["response_and_receipt_added_chars"] = (
                    after_commit - request_chars
                )
        if self._paused is activation:
            self._paused = None
        self._journal_activation(activation)

    def _journal_activation(self, activation: Activation) -> None:
        journal = self._journal
        if journal is None:
            return
        error = None
        if activation.error_kind:
            error = {"kind": activation.error_kind, "message": activation.error_message}
        journal.activation(
            {
                "id": activation.id,
                "session_id": self.session_id,
                "input_event_ids": list(activation.input_event_ids),
                "transcript_version": activation.transcript_version,
                "status": activation.status,
                "started_at": activation.started_at,
                "ended_at": activation.ended_at,
                "model": self.model or "scripted",
                "usage": activation.context_metrics.get("provider_usage"),
                "context": activation.context_metrics,
                "error": error,
            }
        )

    def _latest_provider_usage(self) -> dict[str, Any] | None:
        for activation in reversed(self.activations):
            usage = activation.context_metrics.get("provider_usage")
            if isinstance(usage, dict):
                return usage
        return None


    def _journal_tool(
        self,
        activation: Activation,
        tool_call_id: str,
        execution_id: str | None,
        status: str,
        reason: str | None,
    ) -> None:
        journal = self._journal
        if journal is None:
            return
        journal.tool_call(
            {
                "session_id": self.session_id,
                "activation_id": activation.id,
                "tool_call_id": tool_call_id,
                "execution_id": execution_id,
                "status": status,
                "reason": reason,
            }
        )
