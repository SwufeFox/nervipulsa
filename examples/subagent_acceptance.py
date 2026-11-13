"""Repeatable live CLI acceptance; requires NV_BASE_URL, NV_API_KEY and NV_MODEL.

Run: python examples/subagent_acceptance.py [--max-activations 12]
         [--run-timeout 180] [--process-timeout 210] [--pytest-timeout 60]
NV_PROVIDER defaults to openai. Credentials are passed only in the environment.
Every invocation creates .scratch/subagent-acceptance-<uuid>/ containing report.json,
workspace/ (fixture and runtime journal), verifier/ (independent pytest fixture),
and artifacts/ (sanitized argv, stdout, parsed JSON, stderr, and journal snapshots).

The CLI process timeout supervises startup, execution and teardown, with up to
five additional seconds for OS tree cleanup. The shorter run timeout initiates
orderly shutdown. A local stalled HTTP scenario verifies forced termination;
the remaining checks use the live model. Runtime completion is separate from
independently verified task success. This paid check is outside the test suite.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import math
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
DEADLINE_NOTE = (
    "--run-timeout initiates orderly shutdown; --process-timeout also bounds "
    "runtime startup and teardown, with up to 5s for forced OS tree cleanup."
)
CRITERIA = {
    "configuration": [],
    "first.json_object": ["configuration"],
    "first.exit_coherent": ["first.json_object"],
    "first.message_contract": ["first.json_object"],
    "first.supervision": ["first.json_object"],
    "first.runtime_completed": ["first.exit_coherent"],
    "fixture.calc_changed": ["configuration"],
    "fixture.test_unchanged": ["configuration"],
    "fixture.independent_pytest": ["configuration"],
    "journal.first_readable": ["configuration"],
    "resume.json_object": ["first.runtime_completed", "journal.first_readable"],
    "resume.exit_coherent": ["resume.json_object"],
    "resume.message_contract": ["resume.json_object"],
    "resume.supervision": ["resume.json_object"],
    "resume.runtime_completed": ["resume.exit_coherent"],
    "resume.transcript_restored": ["resume.json_object"],
    "resume.marker_recalled": ["resume.runtime_completed"],
    "journal.resume_readable": ["resume.json_object"],
    "resume.no_tools": ["journal.resume_readable"],
    "resume.prior_rows_preserved": ["journal.resume_readable"],
    "resume.journal_extended": ["journal.resume_readable"],
    "reuse.json_object": ["journal.first_readable"],
    "reuse.exit_coherent": ["reuse.json_object"],
    "reuse.message_contract": ["reuse.json_object"],
    "reuse.supervision": ["reuse.json_object"],
    "reuse.session_exists": ["reuse.json_object"],
    "journal.reuse_readable": ["reuse.json_object"],
    "reuse.prior_rows_preserved": ["journal.reuse_readable"],
    "fixture.test_still_unchanged": ["configuration"],
    "deadline.json_object": ["configuration"],
    "deadline.exit_coherent": ["deadline.json_object"],
    "deadline.message_contract": ["deadline.json_object"],
    "deadline.supervision": ["deadline.json_object"],
    "deadline.request_started": ["configuration"],
    "deadline.forced_timeout": ["deadline.exit_coherent"],
    "deadline.bounded_exit": ["deadline.exit_coherent"],
    "harness_execution": [],
}


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def positive_seconds(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return parsed


class Acceptance:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.directory = ROOT / ".scratch" / f"subagent-acceptance-{uuid.uuid4().hex}"
        self.workspace = self.directory / "workspace"
        self.artifacts = self.directory / "artifacts"
        self.workspace.mkdir(parents=True)
        self.artifacts.mkdir()
        (self.directory / "config").mkdir()
        self.session_id = f"acceptance-{uuid.uuid4().hex}"
        self.marker = f"memory-{uuid.uuid4().hex}"
        # Include escaped representations, so JSON string escaping cannot leak a key.
        sensitive = {os.environ["NV_API_KEY"]} if os.environ.get("NV_API_KEY") else set()
        self.secrets = sorted(
            sensitive | {json.dumps(value, ensure_ascii=True)[1:-1] for value in sensitive}
            | {json.dumps(value, ensure_ascii=False)[1:-1] for value in sensitive},
            key=len, reverse=True,
        )
        self.criteria = {
            name: {"status": "skipped", "required": True, "depends_on": dependencies,
                   "detail": "not reached; see failed dependencies or harness_execution"}
            for name, dependencies in CRITERIA.items()
        }
        self.processes: dict[str, dict] = {}
        self.env = {name: value for name, value in os.environ.items()
                    if not name.startswith(("NV_", "NERVIPULSA_"))}
        self.env.update({
            "NERVIPULSA_CONFIG_DIR": str(self.directory / "config"),
            "NERVIPULSA_PROVIDER": os.environ.get("NV_PROVIDER") or "openai",
            "NERVIPULSA_BASE_URL": os.environ.get("NV_BASE_URL", ""),
            "NERVIPULSA_API_KEY": os.environ.get("NV_API_KEY", ""),
            "NERVIPULSA_MODEL": os.environ.get("NV_MODEL", ""),
            "PYTHONPATH": str(ROOT),
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        })

    def sanitize(self, text: str) -> str:
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        return text

    def save_text(self, name: str, text: str) -> None:
        (self.artifacts / name).write_text(self.sanitize(text), encoding="utf-8")

    def save_json(self, name: str, value: object) -> None:
        # Sanitize strings before serialization to preserve valid JSON even when
        # a credential contains punctuation or JSON escape sequences.
        def clean(item):
            if isinstance(item, str):
                return self.sanitize(item)
            if isinstance(item, list):
                return [clean(entry) for entry in item]
            if isinstance(item, dict):
                return {self.sanitize(str(key)): clean(entry) for key, entry in item.items()}
            return item
        (self.artifacts / name).write_text(
            json.dumps(clean(value), indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    def check(self, name: str, passed: bool, detail: object) -> bool:
        self.criteria[name].update(status="passed" if passed else "failed", detail=detail)
        return passed

    def passed(self, name: str) -> bool:
        return self.criteria[name]["status"] == "passed"

    def command(self, prompt: str, *, resume: bool = False, workspace: Path | None = None,
                run_timeout: float | None = None, process_timeout: float | None = None) -> list[str]:
        command = [
            sys.executable, "-m", "nervipulsa", "--dir", str(workspace or self.workspace),
            "--session-id", self.session_id, "--max-activations", str(self.args.max_activations),
            "--run-timeout", str(self.args.run_timeout if run_timeout is None else run_timeout),
            "--process-timeout", str(self.args.process_timeout if process_timeout is None else process_timeout),
            "--timeout", "30",
            "--max-timeout", "60", "--prompt", prompt, "--json",
        ]
        if resume:
            command.append("--resume")
        return command

    def run_process(self, name: str, command: list[str], cwd: Path,
                    *, timeout: float | None = None, env: dict | None = None) -> tuple[int | None, str]:
        self.save_json(f"{name}.command.json", {
            "argv": command, "cwd": str(cwd),
            "environment": "Credentials/configuration supplied via environment; values omitted",
            "process_timeout_seconds": timeout,
            "deadline_note": DEADLINE_NOTE if timeout is None else "Timeout bounds this pytest child only",
        })
        started = time.monotonic()
        error = None
        try:
            result = subprocess.run(
                command, cwd=cwd, env=self.env if env is None else env,
                stdin=subprocess.DEVNULL, capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=timeout, check=False,
            )
            code, stdout, stderr = result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired as exc:
            code = None
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            error = "pytest process timeout"
        except OSError as exc:
            code, stdout, stderr = None, "", ""
            error = f"{type(exc).__name__}: {exc}"
        self.save_text(f"{name}.stdout.txt", stdout)
        self.save_text(f"{name}.stderr.txt", stderr)
        self.processes[name] = {
            "returncode": code, "duration_seconds": round(time.monotonic() - started, 3),
            "error": error,
        }
        return code, stdout

    def cli(self, name: str, prompt: str, *, resume: bool = False,
            command: list[str] | None = None, env: dict | None = None) -> dict | None:
        code, stdout = self.run_process(
            name, command if command is not None else self.command(prompt, resume=resume),
            ROOT, env=env,
        )
        try:
            payload = json.loads(stdout)
            if not isinstance(payload, dict):
                raise ValueError("stdout JSON is not an object")
        except ValueError as exc:
            self.check(f"{name}.json_object", False, str(exc))
            return None
        self.save_json(f"{name}.stdout.json", payload)
        self.check(f"{name}.json_object", True, "Exactly one stdout JSON object")
        status = payload.get("status")
        expected_code = 0 if status == "succeeded" else 124 if status == "timeout" else 1
        coherent = (
            status in {"succeeded", "timeout", "rejected", "paused", "failed"}
            and type(payload.get("exit_code")) is int
            and code == payload.get("exit_code") == expected_code
            and payload.get("ok") is (status == "succeeded")
            and payload.get("session_id") == self.session_id
        )
        self.check(f"{name}.exit_coherent", coherent, {
            "process_exit": code, "json_exit": payload.get("exit_code"),
            "status": status, "ok": payload.get("ok"),
        })
        final = payload.get("final_message")
        last = payload.get("last_message")
        message_contract = (
            "final_message" in payload and "last_message" in payload
            and (final is None or isinstance(final, str))
            and (status == "succeeded" or final is None)
            and (last is None or (
                isinstance(last, dict) and isinstance(last.get("text"), str)
                and isinstance(last.get("activation_id"), str)
            ))
            and (final is None or (
                isinstance(last, dict) and last.get("text") == final
                and isinstance(payload.get("activation"), dict)
                and last.get("activation_id") == payload["activation"].get("id")
            ))
        )
        self.check(f"{name}.message_contract", message_contract,
                   "final_message belongs only to success; last_message is null or activation/text")
        supervision = payload.get("supervision")
        self.check(f"{name}.supervision", isinstance(supervision, dict)
                   and supervision.get("tree_stopped") is True,
                   "Supervisor confirms termination of the managed process tree")
        if name in {"first", "resume"}:
            self.check(f"{name}.runtime_completed", coherent and status == "succeeded",
                       "Runtime completion alone does not verify the requested task")
        return payload

    def journal(self, stage: str) -> dict[str, list[dict]] | None:
        path = self.workspace / ".nervipulsa" / "journal.sqlite"
        try:
            with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
                connection.row_factory = sqlite3.Row
                tables = [row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )]
                rows = {}
                for table in tables:
                    quoted = '"' + table.replace('"', '""') + '"'
                    columns = [row[1] for row in connection.execute(f"PRAGMA table_info({quoted})")]
                    if "session_id" in columns:
                        rows[table] = [dict(row) for row in connection.execute(
                            f"SELECT * FROM {quoted} WHERE session_id=?", (self.session_id,)
                        )]
            required = {"events", "activations", "executions", "tool_calls",
                        "durable_transcript_snapshots", "durable_activation_commits",
                        "durable_tool_receipts"}
            if not required.issubset(rows) or not rows["durable_transcript_snapshots"]:
                raise ValueError("missing journal tables or committed transcript snapshot")
            self.save_json(f"journal.{stage}.json", rows)
            self.check(f"journal.{stage}_readable", True,
                       {table: len(entries) for table, entries in rows.items()})
            return rows
        except (OSError, sqlite3.Error, ValueError) as exc:
            self.check(f"journal.{stage}_readable", False, f"{type(exc).__name__}: {exc}")
            return None

    def preserve(self, name: str, before: dict, after: dict) -> None:
        missing = {}
        for table, rows in before.items():
            old = Counter(json.dumps(row, sort_keys=True) for row in rows)
            new = Counter(json.dumps(row, sort_keys=True) for row in after.get(table, []))
            lost = sum((old - new).values())
            if lost:
                missing[table] = lost
        self.check(name, not missing, {"missing_or_changed_rows": missing})

    def verify_fixture(self, original_calc: bytes, original_test: bytes) -> None:
        calc = self.workspace / "calc.py"
        test = self.workspace / "test_calc.py"
        changed = calc.is_file() and calc.read_bytes() != original_calc
        unchanged = test.is_file() and test.read_bytes() == original_test
        self.check("fixture.calc_changed", changed, "Compared calc.py with original fixture bytes")
        self.check("fixture.test_unchanged", unchanged, "Compared test_calc.py with original fixture bytes")
        self.save_json("fixture.hashes.json", {
            "original_calc_sha256": hashlib.sha256(original_calc).hexdigest(),
            "original_test_sha256": hashlib.sha256(original_test).hexdigest(),
            "result_calc_sha256": hashlib.sha256(calc.read_bytes()).hexdigest() if calc.is_file() else None,
            "result_test_sha256": hashlib.sha256(test.read_bytes()).hexdigest() if test.is_file() else None,
        })
        if not calc.is_file():
            self.criteria["fixture.independent_pytest"]["detail"] = "dependent skip: calc.py is missing"
            return
        verifier = self.directory / "verifier"
        verifier.mkdir()
        shutil.copyfile(calc, verifier / "calc.py")
        (verifier / "test_calc.py").write_bytes(original_test)
        # Test only the changed module against pristine tests, without model-added
        # conftest/config files or inherited pytest options and plugins.
        (verifier / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
        test_env = {key: value for key, value in self.env.items()
                    if not key.startswith(("NERVIPULSA_", "PYTEST_"))}
        test_env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
        code, _ = self.run_process(
            "pytest", [sys.executable, "-P", "-m", "pytest", "-q", "-p", "no:cacheprovider",
                       "-c", "pytest.ini", "test_calc.py"], verifier,
            timeout=self.args.pytest_timeout, env=test_env,
        )
        self.check("fixture.independent_pytest", code == 0, {
            "returncode": code, "tests": "pristine test_calc.py in independent verifier directory",
        })

    def verify_process_deadline(self) -> None:
        requested = threading.Event()
        release = threading.Event()

        class StalledProvider(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                requested.set()
                release.wait(15)

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), StalledProvider)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        limit = 3.0
        prompt = "Reply when the provider responds."
        command = self.command(
            prompt, workspace=self.directory / "deadline-workspace",
            run_timeout=0.25, process_timeout=limit,
        )
        env = dict(self.env, NERVIPULSA_PROVIDER="openai", NERVIPULSA_MODEL="deadline-fixture",
                   NERVIPULSA_API_KEY="deadline-fixture",
                   NERVIPULSA_BASE_URL=f"http://127.0.0.1:{server.server_port}/v1")
        try:
            result = self.cli("deadline", prompt, command=command, env=env)
            self.check("deadline.request_started", requested.is_set(),
                       "Local HTTP request entered its blocking handler before termination")
            supervision = result.get("supervision") if result is not None else None
            self.check("deadline.forced_timeout", self.passed("deadline.exit_coherent")
                       and result.get("status") == "timeout" and isinstance(supervision, dict)
                       and supervision.get("timed_out") is True,
                       "Outer supervisor terminated blocking HTTP teardown after the soft deadline")
            duration = self.processes["deadline"]["duration_seconds"]
            self.check("deadline.bounded_exit", duration < limit + 8,
                       {"seconds": duration, "deadline_seconds": limit, "cleanup_allowance_seconds": 5})
        finally:
            release.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def exercise(self) -> None:
        missing = [name for name in ("NV_BASE_URL", "NV_API_KEY", "NV_MODEL")
                   if not os.environ.get(name, "").strip()]
        if not self.check("configuration", not missing, {"missing_environment": missing}):
            return
        self.verify_process_deadline()
        original_calc = (ROOT / "examples" / "bugfix" / "calc.py").read_bytes()
        original_test = (ROOT / "examples" / "bugfix" / "test_calc.py").read_bytes()
        (self.workspace / "calc.py").write_bytes(original_calc)
        (self.workspace / "test_calc.py").write_bytes(original_test)
        prompt = (
            "Fix the bug in calc.py so add(a, b) returns the sum, preserving the two-argument "
            "signature. Read calc.py and test_calc.py, change only calc.py, and run "
            "python -m pytest -q test_calc.py. Do not modify test_calc.py or create other source "
            "or configuration files. Wait for your test result before your final response. "
            f"Remember this exact marker for a later conversation turn: {self.marker}. "
            "Keep the marker in conversation memory only; do not write it to a file. "
            "Do not inspect environment variables or credentials."
        )
        self.cli("first", prompt)
        self.verify_fixture(original_calc, original_test)
        first_rows = self.journal("first")
        latest_rows = first_rows
        if first_rows is not None and self.passed("first.runtime_completed"):
            resumed = self.cli(
                "resume", "Recall the exact marker from my first prompt. Respond with only that "
                "marker, without quotes or explanation. Do not use any tools, Python executions, "
                "file reads, or environment access; use the restored conversation only.", resume=True,
            )
            if resumed is not None:
                restoration = resumed.get("resume")
                self.check("resume.transcript_restored", isinstance(restoration, dict)
                           and restoration.get("restored") is True, restoration)
                final = resumed.get("final_message")
                self.check("resume.marker_recalled", self.passed("resume.runtime_completed")
                           and isinstance(final, str) and final.strip() == self.marker,
                           "Final response must equal the unique first-prompt marker")
            resumed_rows = self.journal("resume")
            if resumed_rows is not None:
                latest_rows = resumed_rows
                self.preserve("resume.prior_rows_preserved", first_rows, resumed_rows)
                new_tools = {
                    table: len(resumed_rows[table]) - len(first_rows[table])
                    for table in ("tool_calls", "durable_tool_receipts", "executions")
                }
                old_event_ids = {row["id"] for row in first_rows["events"]}
                new_python_requests = sum(
                    row["id"] not in old_event_ids and row["type"] == "python.requested"
                    for row in resumed_rows["events"]
                )
                self.check("resume.no_tools", all(count == 0 for count in new_tools.values())
                           and new_python_requests == 0,
                           {"added_rows": new_tools, "new_python_requests": new_python_requests})
                self.check("resume.journal_extended", all(
                    len(resumed_rows[table]) > len(first_rows[table])
                    for table in ("events", "activations", "durable_transcript_snapshots")
                ), "Continuation appends events, activations and transcript snapshots")
        if first_rows is not None:
            refused = self.cli("reuse", "Reply with the word refused. Do not use tools.")
            if refused is not None:
                delivery = refused.get("delivery")
                self.check("reuse.session_exists", self.passed("reuse.exit_coherent")
                           and refused.get("status") == "rejected" and isinstance(delivery, dict)
                           and delivery.get("accepted") is False
                           and delivery.get("reason") == "session_exists",
                           "Existing history requires --resume and must be rejected before model work")
            refused_rows = self.journal("reuse")
            if refused_rows is not None and latest_rows is not None:
                self.preserve("reuse.prior_rows_preserved", latest_rows, refused_rows)
        test = self.workspace / "test_calc.py"
        self.check("fixture.test_still_unchanged", test.is_file() and test.read_bytes() == original_test,
                   "Test fixture still matches original after continuation and refusal")

    def finish(self) -> int:
        failures = [name for name, result in self.criteria.items() if result["status"] == "failed"]
        skipped = [name for name, result in self.criteria.items() if result["status"] == "skipped"]
        verified = all(self.passed(name) for name in (
            "fixture.calc_changed", "fixture.test_unchanged",
            "fixture.independent_pytest", "fixture.test_still_unchanged",
        ))
        report = {
            "ok": not failures and not skipped,
            "runtime_completed": self.passed("first.runtime_completed"),
            "verified_task_success": verified,
            "session_id": self.session_id,
            "artifact_directory": str(self.directory),
            "limits": vars(self.args), "deadline_note": DEADLINE_NOTE,
            "environment_names": ["NV_BASE_URL", "NV_API_KEY", "NV_MODEL", "NV_PROVIDER (optional)"],
            "criteria": self.criteria, "failures": failures, "dependent_skips": skipped,
            "processes": self.processes,
        }
        self.save_json("report.json", report)
        shutil.move(str(self.artifacts / "report.json"), self.directory / "report.json")
        print(json.dumps({key: report[key] for key in (
            "ok", "runtime_completed", "verified_task_success", "artifact_directory", "failures", "dependent_skips"
        )}, ensure_ascii=False))
        return 0 if report["ok"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-activations", type=positive_int, default=12)
    parser.add_argument("--run-timeout", type=positive_seconds, default=180.0)
    parser.add_argument("--process-timeout", type=positive_seconds, default=210.0)
    parser.add_argument("--pytest-timeout", type=positive_seconds, default=60.0)
    args = parser.parse_args()
    acceptance = Acceptance(args)
    print(DEADLINE_NOTE, file=sys.stderr, flush=True)
    try:
        acceptance.exercise()
        acceptance.check("harness_execution", True, "Harness completed without an unexpected exception")
    except Exception as exc:
        acceptance.check("harness_execution", False, f"{type(exc).__name__}: {exc}")
    return acceptance.finish()


if __name__ == "__main__":
    raise SystemExit(main())
