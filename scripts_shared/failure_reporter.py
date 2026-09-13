"""Failure reporting for SchoolNossa pipeline runs.

Sends one event per failed orchestrator run to the SchoolNossa backend's public
`client-errors` edge function (component="pipeline"). The backend stores it in
`failure_events`, emails PERMANENT failures immediately (one email per distinct
failure per 30 minutes) and rolls everything into the daily 08:00 report.
Same design as StoryTeller's failure_reporter; no Resend key is needed locally.

Usage, in an orchestrator's __main__ block:

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts_shared"))
    from failure_reporter import run_with_failure_reporting
    run_with_failure_reporting(main, pipeline="munich")

What gets reported
    PERMANENT  the run raised, exited non-zero, or the orchestrator logged a
               phase failure at ERROR. The first phase-failure line becomes the
               message; up to 40 ERROR lines go in the body so the email says
               which phase broke and why.
    DEGRADED   the run succeeded but enrichment modules logged ERROR lines
               (e.g. per-school lookups). Daily report only.
    nothing    clean runs, and Ctrl+C.

THE ONE RULE: reporting never changes the run's outcome. Every path here swallows
its own exceptions, and the orchestrator's exit code is preserved.

Opt out for local experiments with SCHOOLNOSSA_ALERTS=0.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import socket
import subprocess
import sys
import traceback
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

CLIENT_ERRORS_URL = os.environ.get(
    "SCHOOLNOSSA_CLIENT_ERRORS_URL",
    "https://whzvzoumldeqgyrqlilt.supabase.co/functions/v1/client-errors",
)
# The anon key is public (it ships in the web app); same value as
# upload_to_supabase.SUPABASE_ANON_KEY.
ANON_KEY = os.environ.get("SUPABASE_ANON_KEY") or (
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'
    'eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoenZ6b3VtbGRlcWd5cnFsaWx0Iiwicm9sZSI6ImFub24i'
    'LCJpYXQiOjE3Njg3OTQ0MzEsImV4cCI6MjA4NDM3MDQzMX0.'
    'ex4S1up25OAcGD8hQoOSfzf3NVAG5qCmNriixYfAAKs'
)

_TIMEOUT = 10
_MAX_LOG_LINES = 40
_USER_AGENT = "SchoolNossa-Pipeline/1.0"

logger = logging.getLogger(__name__)


class _ErrorCollector(logging.Handler):
    """Keeps the ERROR/CRITICAL records a run emits, without altering logging."""

    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.records: list[str] = []
        # ERROR lines from the orchestrator's own logger that say something
        # failed ("Phase 5 failed: ...", "<<< Phase 3 FAILED"). Not every
        # orchestrator exits non-zero when a phase fails (Stuttgart exits 0), so
        # these count as a failed run on their own.
        self.phase_failures: list[str] = []
        self.first_exc_type: str | None = None

    def emit(self, record):
        try:
            if record.name == __name__:
                return
            msg = record.getMessage()
            line = "{} {} {}: {}".format(
                datetime.fromtimestamp(record.created).strftime("%H:%M:%S"),
                record.levelname, record.name, msg)
            self.records.append(line[:500])
            if record.name == "__main__" and "fail" in msg.lower():
                self.phase_failures.append(msg[:500])
            if self.first_exc_type is None and record.exc_info and record.exc_info[0]:
                self.first_exc_type = record.exc_info[0].__name__
        except Exception:
            pass


def _git_ref() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True,
            timeout=3, cwd=Path(__file__).resolve().parent)
        branch = out.stdout.strip()
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
            timeout=3, cwd=Path(__file__).resolve().parent)
        return "{}@{}".format(branch, out.stdout.strip()) if branch else None
    except Exception:
        return None


def report(message, *, stage, severity="PERMANENT", error_code=None, error_type=None,
           stack=None, context=None) -> bool:
    """POST one event to client-errors. Returns True on 2xx. Never raises."""
    if os.environ.get("SCHOOLNOSSA_ALERTS", "1") == "0":
        return False
    payload = {
        "component": "pipeline",
        "message": str(message)[:2000],
        "stage": stage,
        "severity": severity,
        "platform": "{} / Python {}".format(platform.platform(), platform.python_version()),
        "app_version": _git_ref(),
        "context": context or {},
    }
    if error_code:
        payload["error_code"] = error_code
    if error_type:
        payload["error_type"] = error_type
    if stack:
        payload["stack"] = str(stack)[-8000:]
    try:
        body = json.dumps(payload, default=str).encode("utf-8")
        if len(body) > 30_000:
            payload["context"] = {"truncated": True}
            body = json.dumps(payload, default=str).encode("utf-8")
        req = urllib.request.Request(
            CLIENT_ERRORS_URL, data=body, method="POST",
            headers={
                "Content-Type": "application/json",
                "apikey": ANON_KEY,
                "Authorization": "Bearer " + ANON_KEY,
                "User-Agent": _USER_AGENT,
            })
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            ok = 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        ok = False
        print("[failure_reporter] client-errors returned HTTP {}".format(e.code), file=sys.stderr)
    except Exception as e:
        ok = False
        print("[failure_reporter] could not report failure: {}: {}".format(
            type(e).__name__, e), file=sys.stderr)
    if ok:
        print("[failure_reporter] reported {} pipeline failure ({})".format(severity, stage),
              file=sys.stderr)
    return ok


def run_with_failure_reporting(main, *, pipeline: str):
    """Run an orchestrator's main(), report a failed run, keep its exit code."""
    collector = _ErrorCollector()
    root = logging.getLogger()
    root.addHandler(collector)
    started = datetime.now()
    exit_code = 0
    exc_info = None

    try:
        main()
    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    except KeyboardInterrupt:
        root.removeHandler(collector)
        raise
    except BaseException as e:  # noqa: BLE001 — re-raised below after reporting
        exit_code = 1
        exc_info = (type(e), e, e.__traceback__)

    try:
        root.removeHandler(collector)
        if exit_code != 0 or exc_info or collector.records:
            duration = str(datetime.now() - started).split(".")[0]
            context = {
                "pipeline": pipeline,
                "argv": sys.argv[1:],
                "exit_code": exit_code,
                "duration": duration,
                "host": socket.gethostname(),
                "error_lines_logged": len(collector.records),
            }
            lines = collector.records[:_MAX_LOG_LINES]
            if exc_info:
                error_type = exc_info[0].__name__
                message = "{} pipeline crashed: {}: {}".format(pipeline, error_type, exc_info[1])
                stack = "".join(traceback.format_exception(*exc_info))
                if lines:
                    stack += "\n--- logged errors before the crash ---\n" + "\n".join(lines)
                report(message, stage=pipeline, error_type=error_type, stack=stack,
                       context=context)
            elif exit_code != 0 or collector.phase_failures:
                if collector.phase_failures:
                    first = collector.phase_failures[0]
                else:
                    first = lines[0].split(": ", 1)[-1] if lines else "no ERROR lines logged"
                message = "{} pipeline failed (exit {}): {}".format(pipeline, exit_code, first)
                context["phase_failures"] = collector.phase_failures[:10]
                report(message, stage=pipeline, error_code="pipeline_run_failed",
                       error_type=collector.first_exc_type or "PipelineRunFailed",
                       stack="\n".join(lines), context=context)
            else:
                message = "{} pipeline finished but logged {} error line(s): {}".format(
                    pipeline, len(collector.records), lines[0].split(": ", 1)[-1])
                report(message, stage=pipeline, severity="DEGRADED",
                       error_code="pipeline_logged_errors", stack="\n".join(lines),
                       context=context)
    except Exception as e:
        print("[failure_reporter] internal error: {}".format(e), file=sys.stderr)

    if exc_info:
        raise exc_info[1].with_traceback(exc_info[2])
    sys.exit(exit_code)
