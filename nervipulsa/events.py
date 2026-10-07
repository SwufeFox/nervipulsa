"""Small, single-loop event kernel with bounded mailboxes."""

from __future__ import annotations

import asyncio
import json
import math
import uuid
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Mapping


SYSTEM_PROMPT = "You are a helpful software engineer assistant."
MAX_EVENT_BYTES = 1024 * 1024
MAX_HANDLER_RESULTS_PER_EXECUTION = 16


class RuntimeState(StrEnum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"


class Lane(StrEnum):
    ORDINARY = "ordinary"
    RESERVED_RESULT = "reserved_result"
    FEEDBACK = "feedback"
    HANDLER_RESULT = "handler_result"


@dataclass(frozen=True, slots=True)
class Event:
    session_id: str
    id: str
    seq: int
    type: str
    source: str
    target: str
    reply_to: str | None
    payload: Mapping[str, Any]
    accepted_at: float


@dataclass(frozen=True, slots=True)
class Delivery:
    accepted: bool
    event_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _Queued:
    event: Event
    lane: Lane


def _event_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate JSON data and make a defensive snapshot at the call boundary."""
    try:
        raw = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        if len(raw.encode("utf-8")) > MAX_EVENT_BYTES:
            raise ValueError("event payload exceeds the size limit")
        result = json.loads(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("payload must be finite JSON data within the event size limit") from exc
    if not isinstance(result, dict):
        raise ValueError("payload must be a JSON object")
    return result


def _valid_payload(event_type: str, data: Mapping[str, Any]) -> bool:
    def text(key: str, *, nonempty: bool = False) -> bool:
        value = data.get(key)
        return isinstance(value, str) and (not nonempty or bool(value.strip()))

    def integer(key: str) -> bool:
        value = data.get(key)
        return isinstance(value, int) and not isinstance(value, bool)

    if event_type == "user.message":
        return text("text", nonempty=True) and len(data["text"].encode("utf-8")) <= 128 * 1024
    if event_type == "assistant.message":
        return text("text") and text("activation_id", nonempty=True)
    if event_type == "python.requested":
        timeout = data.get("timeout")
        return (
            text("code")
            and len(data["code"].encode("utf-8")) <= 512 * 1024
            and isinstance(timeout, (int, float))
            and not isinstance(timeout, bool)
            and math.isfinite(timeout)
            and timeout > 0
        )
    if event_type == "python.started":
        return text("execution_id", nonempty=True) and integer("worker_epoch")
    if event_type == "python.finished":
        expected = data.get("expected_handler_count")
        missing = data.get("missing_handler_ids")
        result_status = data.get("handler_result_status")
        valid_missing = (
            missing is None
            or (
                isinstance(missing, list)
                and len(missing) <= MAX_HANDLER_RESULTS_PER_EXECUTION
                and all(isinstance(item, str) and bool(item.strip()) for item in missing)
                and len(set(missing)) == len(missing)
            )
        )
        valid_status = result_status is None or (
            isinstance(result_status, str)
            and result_status in {"complete", "incomplete", "unknown"}
        )
        if result_status == "incomplete":
            valid_status = valid_status and isinstance(missing, list) and bool(missing)
        if result_status == "complete":
            valid_status = valid_status and isinstance(missing, list) and not missing
        if result_status == "unknown":
            valid_status = valid_status and missing is None
        return (
            isinstance(data.get("status"), str)
            and data.get("status") in {"succeeded", "failed", "timeout", "cancelled"}
            and text("stdout")
            and text("stderr")
            and isinstance(data.get("duration_ms"), (int, float))
            and integer("worker_epoch")
            and isinstance(data.get("namespace_reset"), bool)
            and (
                "expected_handler_count" not in data
                or (
                    isinstance(expected, int)
                    and not isinstance(expected, bool)
                    and 0 <= expected <= MAX_HANDLER_RESULTS_PER_EXECUTION
                )
            )
            and valid_missing
            and valid_status
        )
    if event_type == "python.cancel":
        return text("execution_id", nonempty=True) and text("reason")
    if event_type == "python.cancel_result":
        return (
            data.get("status") in {"requested", "already_finished", "not_found"}
            and text("execution_id", nonempty=True)
        )
    if event_type == "agent.feedback":
        return text("activation_id", nonempty=True) and isinstance(data.get("rejections"), list)
    if event_type == "agent.error":
        return text("message", nonempty=True) and text("kind", nonempty=True)
    if event_type == "agent.retry":
        return True
    if event_type == "agent.handler_fired":
        return (
            text("handler_id", nonempty=True)
            and isinstance(data.get("trigger"), dict)
            and text("result")
            and integer("worker_epoch")
        )
    if event_type == "python.environment_changed":
        return (
            integer("old_epoch")
            and integer("new_epoch")
            and text("reason", nonempty=True)
        )
    if event_type == "python.environment_discovered":
        return (
            text("activation_id", nonempty=True)
            and text("tool_call_id", nonempty=True)
            and isinstance(data.get("read_only"), bool)
            and isinstance(data.get("python"), str)
            and isinstance(data.get("workspace"), str)
            and isinstance(data.get("project_files"), list)
            and isinstance(data.get("modules"), list)
            and data.get("install_supported") is False
            and data.get("status", "succeeded") in {"succeeded", "failed"}
            and (data.get("status", "succeeded") != "failed" or isinstance(data.get("error"), str))
        )
    if event_type == "session.shutdown":
        return True
    return False


class Mailbox:
    """One named receiver port; offer is synchronous and never waits for space."""

    def __init__(
        self,
        name: str,
        *,
        ordinary_limit: int = 64,
        result_limit: int = 4,
        feedback_limit: int = 1,
        handler_result_limit: int = 16,
    ) -> None:
        self.name = name
        self.ordinary_limit = ordinary_limit
        self.result_limit = result_limit
        self.feedback_limit = feedback_limit
        self.handler_result_limit = handler_result_limit
        self._items: deque[_Queued] = deque()
        self._ordinary_size = 0
        self._leased: dict[str, _Queued] = {}
        self._wake = asyncio.Event()
        self._reservations: set[str] = set()
        self._handler_slots: dict[str, list[int]] = {}  # available, queued or leased
        self._handler_events: dict[str, str] = {}  # event id -> execution id
        self._feedback_ids: set[str] = set()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def size(self) -> int:
        return len(self._items)

    @property
    def ordinary_size(self) -> int:
        return self._ordinary_size

    @property
    def reserved_size(self) -> int:
        return len(self._reservations)

    @property
    def handler_reserved_size(self) -> int:
        return sum(available + outstanding for available, outstanding in self._handler_slots.values())

    def reserve_handlers(self, execution_id: str, count: int) -> bool:
        if (
            self._closed
            or count < 0
            or count > MAX_HANDLER_RESULTS_PER_EXECUTION
            or execution_id in self._handler_slots
        ):
            return False
        if self.handler_reserved_size + count > self.handler_result_limit * self.result_limit:
            return False
        self._handler_slots[execution_id] = [count, 0]
        return True

    def settle_handler_reservation(self, execution_id: str, expected_count: int) -> None:
        slots = self._handler_slots.get(execution_id)
        if slots is None:
            return
        slots[0] = min(slots[0], max(0, expected_count - slots[1]))
        if slots == [0, 0]:
            self._handler_slots.pop(execution_id, None)

    def release_handlers(self, execution_id: str) -> None:
        self._handler_slots.pop(execution_id, None)

    def can_offer(self, lane: Lane, *, key: str | None = None) -> str | None:
        if self._closed:
            return "receiver_unavailable"
        if lane is Lane.ORDINARY and self.ordinary_size >= self.ordinary_limit:
            return "capacity_exceeded"
        if lane is Lane.RESERVED_RESULT:
            if not key or key not in self._reservations:
                return "invalid_payload"
        if lane is Lane.HANDLER_RESULT:
            if not key or key not in self._handler_slots or self._handler_slots[key][0] < 1:
                return "capacity_exceeded"
        if lane is Lane.FEEDBACK:
            if len(self._feedback_ids) >= self.feedback_limit:
                return "capacity_exceeded"
            if not key or key in self._feedback_ids:
                return "invalid_payload"
        return None

    def reserve_terminal(self, execution_id: str) -> bool:
        if self._closed or execution_id in self._reservations:
            return False
        if len(self._reservations) >= self.result_limit:
            return False
        self._reservations.add(execution_id)
        return True

    def release_terminal(self, execution_id: str) -> None:
        self._reservations.discard(execution_id)

    def offer(self, event: Event, lane: Lane, *, key: str | None = None) -> Delivery:
        reason = self.can_offer(lane, key=key)
        if reason:
            return Delivery(False, event.id, reason)
        if event.type == "agent.handler_fired":
            if lane not in {Lane.HANDLER_RESULT, Lane.ORDINARY}:
                return Delivery(False, event.id, "invalid_payload")
            handler_key = key if lane is Lane.HANDLER_RESULT else event.reply_to
            if not handler_key or event.reply_to != handler_key:
                return Delivery(False, event.id, "invalid_payload")
            slots = self._handler_slots.get(handler_key)
            if slots is None or slots[0] < 1:
                return Delivery(False, event.id, "capacity_exceeded")
        elif lane is Lane.HANDLER_RESULT:
            return Delivery(False, event.id, "invalid_payload")
        if lane is Lane.RESERVED_RESULT and event.type == "python.finished":
            if event.reply_to != key:
                return Delivery(False, event.id, "invalid_payload")
            expected = event.payload.get("expected_handler_count")
            if isinstance(expected, int) and not isinstance(expected, bool):
                self.settle_handler_reservation(event.reply_to, expected)
        if event.type == "agent.handler_fired":
            assert event.reply_to is not None
            slots = self._handler_slots[event.reply_to]
            slots[0] -= 1
            slots[1] += 1
            self._handler_events[event.id] = event.reply_to
        if lane is Lane.FEEDBACK:
            assert key is not None
            self._feedback_ids.add(key)
        self._items.append(_Queued(event, lane))
        if lane is Lane.ORDINARY:
            self._ordinary_size += 1
        self._wake.set()
        return Delivery(True, event.id)

    async def take_batch(self, limit: int = 32) -> list[Event]:
        if limit < 1:
            raise ValueError("batch limit must be positive")
        while not self._items:
            if self._closed:
                return []
            self._wake.clear()
            if self._items:
                break
            await self._wake.wait()
        result: list[Event] = []
        while self._items and len(result) < limit:
            item = self._items.popleft()
            if item.lane is Lane.ORDINARY:
                self._ordinary_size -= 1
            self._leased[item.event.id] = item
            result.append(item.event)
        result.sort(key=lambda event: event.seq)
        if not self._items:
            self._wake.clear()
        return result

    def drain_available(self, limit: int) -> list[Event]:
        """Pop up to `limit` events already waiting. Never waits for new input."""
        if limit < 1:
            return []
        result: list[Event] = []
        while self._items and len(result) < limit:
            item = self._items.popleft()
            if item.lane is Lane.ORDINARY:
                self._ordinary_size -= 1
            self._leased[item.event.id] = item
            result.append(item.event)
        result.sort(key=lambda event: event.seq)
        if not self._items:
            self._wake.clear()
        return result

    def mark_consumed(self, event: Event) -> None:
        execution_id = self._handler_events.pop(event.id, None)
        if execution_id is not None:
            slots = self._handler_slots.get(execution_id)
            if slots is not None:
                slots[1] = max(0, slots[1] - 1)
                if slots == [0, 0]:
                    self._handler_slots.pop(execution_id, None)
        if event.type == "python.finished" and event.reply_to:
            self.release_terminal(event.reply_to)
            self.settle_handler_reservation(event.reply_to, 0)
        if event.type == "agent.feedback":
            activation_id = event.payload.get("activation_id")
            if isinstance(activation_id, str):
                self._feedback_ids.discard(activation_id)

    def close(self) -> None:
        self._closed = True
        self._items.clear()
        self._ordinary_size = 0
        self._leased.clear()
        self._reservations.clear()
        self._handler_slots.clear()
        self._handler_events.clear()
        self._feedback_ids.clear()
        self._wake.set()


class Bus:
    """Event router owned by one asyncio loop."""

    def __init__(self, *, session_id: str | None = None) -> None:
        self.session_id = session_id or uuid.uuid4().hex
        self.state = RuntimeState.CREATED
        self._seq = 0
        self._routes: dict[str, Mailbox] = {}
        self._mailboxes: dict[str, Mailbox] = {}
        self._observers: list[Callable[[Event], None]] = []
        self._delivery_sinks: list[Callable[[Event | None, str, Delivery], None]] = []

    @property
    def seq(self) -> int:
        return self._seq

    def register(self, event_type: str, mailbox: Mailbox) -> None:
        if self.state is not RuntimeState.CREATED:
            raise RuntimeError("event routes are immutable after the runtime starts")
        if event_type in self._routes:
            raise ValueError(f"route already registered for {event_type}")
        existing = self._mailboxes.get(mailbox.name)
        if existing is not None and existing is not mailbox:
            raise ValueError(f"mailbox name already in use: {mailbox.name}")
        self._mailboxes[mailbox.name] = mailbox
        self._routes[event_type] = mailbox

    def start(self) -> None:
        if self.state is not RuntimeState.CREATED:
            raise RuntimeError(f"cannot start runtime in {self.state} state")
        self.state = RuntimeState.RUNNING

    def add_observer(self, observer: Callable[[Event], None]) -> None:
        self._observers.append(observer)

    def add_delivery_sink(self, sink: Callable[[Event | None, str, Delivery], None]) -> None:
        self._delivery_sinks.append(sink)

    def emitter(self, source: str) -> "Emitter":
        return Emitter(self, source)

    def reserve_result(self, receiver: str, execution_id: str) -> bool:
        mailbox = self._mailboxes.get(receiver)
        return bool(mailbox and mailbox.reserve_terminal(execution_id))

    def reserve_handlers(self, receiver: str, execution_id: str, count: int) -> bool:
        mailbox = self._mailboxes.get(receiver)
        return bool(mailbox and mailbox.reserve_handlers(execution_id, count))

    def settle_handlers(self, receiver: str, execution_id: str, expected_count: int) -> None:
        mailbox = self._mailboxes.get(receiver)
        if mailbox:
            mailbox.settle_handler_reservation(execution_id, expected_count)

    def release_handlers(self, receiver: str, execution_id: str) -> None:
        mailbox = self._mailboxes.get(receiver)
        if mailbox:
            mailbox.release_handlers(execution_id)

    def call(
        self,
        source: str,
        event_type: str,
        payload: Mapping[str, Any],
        *,
        reply_to: str | None = None,
        lane: Lane = Lane.ORDINARY,
        lane_key: str | None = None,
        reserve_result_for: str | None = None,
        reserve_handler_slots: int = 0,
    ) -> Delivery:
        if self.state is RuntimeState.CLOSED:
            return self._reject(None, "unavailable", Delivery(False, reason="runtime_closing"))
        if self.state is RuntimeState.DRAINING:
            if event_type in {"user.message", "python.requested", "agent.retry"}:
                return self._reject(None, "unavailable", Delivery(False, reason="runtime_closing"))
        elif self.state is not RuntimeState.RUNNING:
            return self._reject(None, "unavailable", Delivery(False, reason="receiver_unavailable"))

        receiver = self._routes.get(event_type)
        if receiver is None:
            return self._reject(None, "unavailable", Delivery(False, reason="unknown_route"))
        try:
            snapshot = _event_payload(payload)
        except ValueError:
            return self._reject(None, receiver.name, Delivery(False, reason="invalid_payload"))
        if not _valid_payload(event_type, snapshot):
            return self._reject(None, receiver.name, Delivery(False, reason="invalid_payload"))
        if receiver.closed:
            return self._reject(None, receiver.name, Delivery(False, reason="receiver_unavailable"))

        # Check admission before allocating a sequence or reserving downstream capacity.
        reason = receiver.can_offer(lane, key=lane_key)
        if reason:
            return self._reject(None, receiver.name, Delivery(False, reason=reason))

        self._seq += 1
        event_id = f"evt_{self._seq:08d}"
        reservation = None
        handler_reservation = None
        if reserve_result_for:
            reservation_box = self._mailboxes.get(reserve_result_for)
            if not reservation_box or not reservation_box.reserve_terminal(event_id):
                self._seq -= 1
                return self._reject(None, receiver.name, Delivery(False, reason="capacity_exceeded"))
            reservation = (reservation_box, event_id)
        if reserve_handler_slots:
            reservation_box = self._mailboxes.get(reserve_result_for or receiver.name)
            if (
                not reservation_box
                or not reservation_box.reserve_handlers(event_id, reserve_handler_slots)
            ):
                self._seq -= 1
                if reservation:
                    reservation[0].release_terminal(event_id)
                return self._reject(None, receiver.name, Delivery(False, reason="capacity_exceeded"))
            handler_reservation = (reservation_box, event_id)

        # _event_payload already creates a detached JSON snapshot, so keep that
        # owned object instead of traversing large code/output payloads twice.
        event = Event(
            session_id=self.session_id,
            id=event_id,
            seq=self._seq,
            type=event_type,
            source=source,
            target=receiver.name,
            reply_to=reply_to,
            payload=snapshot,
            accepted_at=asyncio.get_running_loop().time(),
        )
        result = receiver.offer(event, lane, key=lane_key)
        if not result.accepted:
            self._seq -= 1
            if reservation:
                reservation[0].release_terminal(reservation[1])
            if handler_reservation:
                handler_reservation[0].release_handlers(handler_reservation[1])
            # The provisional id was rolled back and may be reused. Do not publish it.
            return self._reject(None, receiver.name, Delivery(False, None, result.reason))

        for observer in tuple(self._observers):
            try:
                observer(event)
            except Exception:
                # Observation is deliberately outside the execution delivery path.
                pass
        self._reject(event, receiver.name, result)
        return result

    def _reject(self, event: Event | None, receiver: str, delivery: Delivery) -> Delivery:
        for sink in tuple(self._delivery_sinks):
            try:
                sink(event, receiver, delivery)
            except Exception:
                pass
        return delivery

    def begin_draining(self) -> None:
        if self.state is RuntimeState.RUNNING:
            self.state = RuntimeState.DRAINING

    def close(self) -> None:
        self.state = RuntimeState.CLOSED
        for mailbox in self._mailboxes.values():
            mailbox.close()


class Emitter:
    """Producer identity is bound once and cannot be inferred from payload text."""

    def __init__(self, bus: Bus, source: str) -> None:
        self._bus = bus
        self.source = source

    def settle_handlers(self, execution_id: str, expected_count: int) -> None:
        self._bus.settle_handlers("llm", execution_id, expected_count)

    def release_handlers(self, execution_id: str) -> None:
        self._bus.release_handlers("llm", execution_id)

    def call(
        self,
        event_type: str,
        payload: Mapping[str, Any],
        *,
        reply_to: str | None = None,
        lane: Lane = Lane.ORDINARY,
        lane_key: str | None = None,
        reserve_result_for: str | None = None,
        reserve_handler_slots: int = 0,
    ) -> Delivery:
        return self._bus.call(
            self.source,
            event_type,
            payload,
            reply_to=reply_to,
            lane=lane,
            lane_key=lane_key,
            reserve_result_for=reserve_result_for,
            reserve_handler_slots=reserve_handler_slots,
        )
