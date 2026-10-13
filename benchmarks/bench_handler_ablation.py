"""Mechanism microbenchmark for Python tool completion handlers.

This explicitly operationalized benchmark compares three PythonExecTool invocation
mechanisms using a real Runtime and PythonHost with a deterministic ScriptedBackend.
It does not measure model quality, tokens, production task success rate, or concurrent
workers. Run: python benchmarks/bench_handler_ablation.py --blocks 5 --seed 20261013
       python benchmarks/bench_handler_ablation.py --blocks 5 --seed 20261013 --json benchmarks/handler_ablation.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import platform
import random
import statistics
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nervipulsa.config import Settings  # noqa: E402
from nervipulsa.events import Event  # noqa: E402
from nervipulsa.ports import ToolContext  # noqa: E402
from nervipulsa.providers import ModelRequest, ScriptedBackend, text_response, tool_response  # noqa: E402
from nervipulsa.runtime import Runtime  # noqa: E402
from nervipulsa.tools import DefaultToolCatalog, PythonExecTool  # noqa: E402

STATUS_TEXT = "status-check: report the current runtime state"
SLEEP_SECONDS = 0.22
MODES = ("A0", "A1", "A2")


class ToolWrapper(PythonExecTool):
    def __init__(self, mode: str, runtime_ref: dict[str, Runtime]) -> None:
        self.mode = mode
        self.runtime_ref = runtime_ref

    async def invoke(self, arguments: dict[str, Any], *, context: ToolContext,
                     activation: Any, call: Any) -> tuple[dict[str, Any], str]:
        code = arguments["code"]
        if self.mode == "A2":
            # The callback is part of this trial's executed code. Its closure removes
            # exactly its own registration after the completion observation fires.
            code += "\n_handle = None\ndef _once(event):\n    off_finished(_handle)\n_handle = on_finished(_once)\n"
            arguments = {**arguments, "code": code}
        result, status = await super().invoke(arguments, context=context, activation=activation, call=call)
        if self.mode != "A0" or status != "accepted":
            return result, status

        execution_id = str(result["execution_id"])
        runtime = self.runtime_ref["runtime"]
        loop = asyncio.get_running_loop()
        terminal: asyncio.Future[Event] = loop.create_future()

        def observe(event: Event) -> None:
            if event.type == "python.finished" and event.reply_to == execution_id and not terminal.done():
                terminal.set_result(event)

        # Install observer, then inspect trace to close the submit/observer race.
        runtime.bus.add_observer(observe)
        already_finished = next((e for e in reversed(runtime.trace)
                                 if e.type == "python.finished" and e.reply_to == execution_id), None)
        if already_finished is not None and not terminal.done():
            terminal.set_result(already_finished)
        try:
            event = await asyncio.wait_for(terminal, timeout=15)
            return {"status": event.payload.get("status", "failed"), "execution_id": execution_id}, "accepted"
        finally:
            observers = runtime.bus._observers
            if observe in observers:
                observers.remove(observe)


async def run_trial(mode: str, block: int, order: int) -> dict[str, Any]:
    task_id = f"handler-ablation-b{block:02d}"
    core_code = f"import time\ntime.sleep({SLEEP_SECONDS})\nprint({('MARKER-' + task_id)!r})\n"
    responder_seen: dict[str, float] = {}
    submitted_at: dict[str, float] = {}
    started_at: dict[str, float] = {}
    execution_ids: dict[str, str] = {}

    def respond(request: ModelRequest):
        user_texts = [str(m.get("content") or "") for m in request.messages if m.get("role") == "user"]
        if any(STATUS_TEXT in text and task_id not in responder_seen for text in user_texts):
            responder_seen[task_id] = time.perf_counter()
            return text_response("status observed")
        if any("run task " + task_id in text for text in user_texts):
            return tool_response((f"call-{task_id}", core_code, None))
        return text_response("status observed")

    backend = ScriptedBackend(respond)
    with tempfile.TemporaryDirectory(
        prefix="nervipulsa-handler-ablation-",
        dir=str(Path(__file__).resolve().parents[1]),
    ) as temp_dir:
        workspace = Path(temp_dir)
        settings = Settings(model="scripted", workspace=str(workspace), max_timeout=10, default_timeout=10)
        settings.context_limit = 2_000_000
        runtime_ref: dict[str, Runtime] = {}
        catalog = DefaultToolCatalog(str(workspace), tools=[ToolWrapper(mode, runtime_ref)])
        runtime = Runtime(settings, backend, workspace, echo=lambda _text: None, tool_catalog=catalog)
        runtime_ref["runtime"] = runtime

        def on_event(event: Event) -> None:
            if event.type == "python.started" and event.reply_to and task_id not in started_at:
                started_at[task_id] = time.perf_counter()
                execution_ids[task_id] = event.reply_to
                # Submit status immediately in the started observer callback.
                submitted_at[task_id] = time.perf_counter()
                runtime.submit_text(STATUS_TEXT)
        runtime.add_listener(on_event)
        await runtime.start()
        try:
            runtime.submit_text("run task " + task_id)
            deadline = time.perf_counter() + 20
            while time.perf_counter() < deadline:
                matching_finished = [e for e in runtime.trace if e.type == "python.finished" and e.reply_to == execution_ids.get(task_id)]
                if matching_finished and task_id in responder_seen and not runtime.actor.busy and runtime.llm_box.size == 0:
                    break
                await asyncio.sleep(0.003)
            else:
                raise TimeoutError(f"trial did not settle: {task_id} {mode}")
            finished = next(e for e in runtime.trace if e.type == "python.finished" and e.reply_to == execution_ids[task_id])
            # Both event timestamps use the same runtime clock.
            requested = next(e for e in runtime.trace if e.type == "python.requested" and e.id == execution_ids[task_id])
            accepted_to_finished_ms = max(0.0, (finished.accepted_at - requested.accepted_at) * 1000)
            event_count = sum(e.type == "agent.handler_fired" and e.reply_to == execution_ids[task_id] for e in runtime.trace)
            succeeded = finished.payload.get("status") == "succeeded" and ("MARKER-" + task_id) in str(finished.payload.get("stdout", ""))
            return {
                "block": block, "task_id": task_id, "mode": mode,
                "random_order_position": order,
                "status_check_text": STATUS_TEXT,
                "status_submit_to_backend_first_seen_ms": round((responder_seen[task_id] - submitted_at[task_id]) * 1000, 3),
                "python_accepted_to_finished_ms": round(accepted_to_finished_ms, 3),
                "handler_event_count": event_count,
                "backend_calls": backend.calls,
                "success": bool(succeeded),
                "python_status": finished.payload.get("status"),
                "execution_id": execution_ids[task_id],
            }
        finally:
            await runtime.shutdown()


async def measure(blocks: int, seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    trials: list[dict[str, Any]] = []
    paired: list[dict[str, Any]] = []
    for block in range(1, blocks + 1):
        order = list(MODES)
        rng.shuffle(order)
        results = {}
        for position, mode in enumerate(order, 1):
            trial = await run_trial(mode, block, position)
            trials.append(trial)
            results[mode] = trial
        for comparison_mode, baseline_mode in (("A1", "A0"), ("A2", "A0"), ("A2", "A1")):
            baseline = results[baseline_mode]
            comparison = results[comparison_mode]
            paired.append({
                "block": block,
                "comparison": comparison_mode,
                "baseline": baseline_mode,
                "status_submit_to_backend_first_seen_delta_ms": round(
                    comparison["status_submit_to_backend_first_seen_ms"]
                    - baseline["status_submit_to_backend_first_seen_ms"], 3
                ),
                "python_accepted_to_finished_delta_ms": round(
                    comparison["python_accepted_to_finished_ms"]
                    - baseline["python_accepted_to_finished_ms"], 3
                ),
                "handler_event_count_delta": comparison["handler_event_count"]
                - baseline["handler_event_count"],
                "backend_calls_delta": comparison["backend_calls"] - baseline["backend_calls"],
                "success_delta": int(comparison["success"]) - int(baseline["success"]),
            })
    summaries: dict[str, dict[str, Any]] = {}
    for mode in MODES:
        mode_trials = [trial for trial in trials if trial["mode"] == mode]
        status_latencies = [trial["status_submit_to_backend_first_seen_ms"] for trial in mode_trials]
        completion_latencies = [trial["python_accepted_to_finished_ms"] for trial in mode_trials]
        summaries[mode] = {
            "trials": len(mode_trials),
            "successes": sum(bool(trial["success"]) for trial in mode_trials),
            "status_latency_mean_ms": round(statistics.mean(status_latencies), 3),
            "status_latency_median_ms": round(statistics.median(status_latencies), 3),
            "status_latency_min_ms": round(min(status_latencies), 3),
            "status_latency_max_ms": round(max(status_latencies), 3),
            "python_accepted_to_finished_mean_ms": round(statistics.mean(completion_latencies), 3),
            "handler_event_count_total": sum(int(trial["handler_event_count"]) for trial in mode_trials),
            "backend_calls_mean": round(statistics.mean(int(trial["backend_calls"]) for trial in mode_trials), 3),
        }
    return {
        "benchmark": "PythonExecTool handler ablation mechanism microbenchmark",
        "operationalization": "A0 awaits matching python.finished in a custom tool wrapper; A1 returns accepted immediately without registering a handler; A2 returns accepted immediately and registers a one-shot on_finished callback that calls off_finished(handle). The normal python.finished event remains routed to the actor in all modes.",
        "scope": "Uses ScriptedBackend and real Runtime/PythonHost. Does not measure model quality, tokens, production task success rate, or concurrent workers.",
        "task_definition": {
            "same_task_ids_within_each_block": True,
            "same_python_sleep_seconds": SLEEP_SECONDS,
            "same_stdout_marker_per_task_across_modes": True,
            "status_check_text": STATUS_TEXT,
            "status_submitted_immediately_on_python_started": True,
        },
        "configuration": {"blocks": blocks, "seed": seed, "modes_randomized_per_block": True},
        "environment": {
            "python": sys.version.split()[0],
            "platform": sys.platform,
            "os": platform.platform(),
            "architecture": platform.machine(),
            "processor": platform.processor(),
        },
        "summary": {
            "trials": len(trials),
            "successes": sum(bool(trial["success"]) for trial in trials),
            "paired_comparisons": len(paired),
        },
        "summary_by_mode": summaries,
        "trials": trials,
        "paired_differences": paired,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--blocks", type=int, default=5)
    parser.add_argument("--seed", type=int, default=20261013)
    parser.add_argument("--json", default="benchmarks/handler_ablation.json")
    args = parser.parse_args(argv)
    if args.blocks < 5:
        parser.error("--blocks must be at least 5 for formal measurement")
    result = asyncio.run(measure(args.blocks, args.seed))
    result["captured_at_local"] = datetime.now().astimezone().isoformat(timespec="seconds")
    result["reproduction_command"] = (
        f"python benchmarks/bench_handler_ablation.py --blocks {args.blocks} "
        f"--seed {args.seed} --json {args.json}"
    )
    rendered = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    path = Path(args.json)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
