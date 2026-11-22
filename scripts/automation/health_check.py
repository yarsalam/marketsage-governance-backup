#!/usr/bin/env python3
"""Read-only, aggregate-only GitHub Actions health observer."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

WATCHDOG_NAME = "MarketSage Infrastructure Watchdog"
CI_NAME = "MarketSage Infrastructure CI"
INFRA_NAMES = {WATCHDOG_NAME, CI_NAME}
MAX_QUEUE_AGE_MINUTES = 20
MAX_RUN_AGE_MINUTES = 20
MAX_JOB_AGE_MINUTES = 12
MAX_SCHEDULE_AGE_HOURS = 8
MAX_ACTIVE_RUNS_TO_INSPECT = 10
PAGE_SIZE = 100
MAX_RUN_PAGES = 5


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_seconds(value: str | None, now: datetime) -> float | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds())


def collect_infrastructure_runs(base_url: str, token: str) -> tuple[list[dict], list[str], bool]:
    """Fetch runs by workflow ID so unrelated high-volume workflows cannot hide them.

    Pagination is deliberately bounded. If the bound is reached, the caller
    marks the health result degraded instead of silently claiming full coverage.
    """
    workflow_payload = github_get_json(
        f"{base_url}/actions/workflows?per_page={PAGE_SIZE}", token
    )
    workflows = workflow_payload.get("workflows", [])
    if not isinstance(workflows, list):
        raise RuntimeError("invalid_workflow_response")

    workflow_ids: dict[str, int] = {}
    for workflow in workflows:
        if workflow.get("name") not in INFRA_NAMES or workflow.get("state") != "active":
            continue
        workflow_id = workflow.get("id")
        if isinstance(workflow_id, int) and workflow_id > 0:
            workflow_ids[workflow["name"]] = workflow_id

    missing_workflows = sorted(INFRA_NAMES - workflow_ids.keys())
    history_truncated = workflow_payload.get("total_count", len(workflows)) > len(workflows)
    runs: list[dict] = []

    for workflow_name in sorted(INFRA_NAMES):
        workflow_id = workflow_ids.get(workflow_name)
        if workflow_id is None:
            continue
        for page in range(1, MAX_RUN_PAGES + 1):
            payload = github_get_json(
                f"{base_url}/actions/workflows/{workflow_id}/runs"
                f"?per_page={PAGE_SIZE}&page={page}",
                token,
            )
            page_runs = payload.get("workflow_runs", [])
            if not isinstance(page_runs, list):
                raise RuntimeError("invalid_workflow_runs_response")
            runs.extend(run for run in page_runs if run.get("name") == workflow_name)
            if len(page_runs) < PAGE_SIZE:
                break
            if page == MAX_RUN_PAGES:
                history_truncated = True

    return runs, missing_workflows, history_truncated


def assess_health(
    runs: list[dict],
    jobs_by_run: dict[str, list[dict]],
    current_run_id: str,
    now: datetime | None = None,
    missing_workflows: list[str] | None = None,
    history_truncated: bool = False,
) -> dict:
    now = now or datetime.now(timezone.utc)
    missing_workflows = missing_workflows or []
    relevant = [
        run for run in runs
        if run.get("name") in INFRA_NAMES
        and str(run.get("id", "")) != str(current_run_id)
    ]

    stale_queued = 0
    stale_running = 0
    stale_jobs = 0
    stale_queued_jobs = 0

    active: list[dict] = []
    for workflow_name in INFRA_NAMES:
        workflow_active = sorted(
            (
                run for run in relevant
                if run.get("name") == workflow_name and run.get("status") == "in_progress"
            ),
            key=lambda run: run.get("created_at", ""),
            reverse=True,
        )
        active.extend(workflow_active[:MAX_ACTIVE_RUNS_TO_INSPECT])

    for run in relevant:
        status = run.get("status")
        if status in {"queued", "waiting", "pending", "requested"}:
            age = _age_seconds(run.get("created_at"), now)
            if age is not None and age > MAX_QUEUE_AGE_MINUTES * 60:
                stale_queued += 1
        elif status == "in_progress":
            age = _age_seconds(run.get("run_started_at") or run.get("created_at"), now)
            if age is not None and age > MAX_RUN_AGE_MINUTES * 60:
                stale_running += 1

    for run in active:
        run_id = str(run.get("id", ""))
        for job in jobs_by_run.get(run_id, []):
            job_status = job.get("status")
            if job_status == "in_progress":
                age = _age_seconds(job.get("started_at"), now)
                if age is not None and age > MAX_JOB_AGE_MINUTES * 60:
                    stale_jobs += 1
            elif job_status in {"queued", "waiting"}:
                run_age = _age_seconds(run.get("created_at"), now)
                if run_age is not None and run_age > MAX_QUEUE_AGE_MINUTES * 60:
                    stale_queued_jobs += 1

    schedule_runs = [
        run for run in relevant
        if run.get("name") == WATCHDOG_NAME and run.get("event") == "schedule"
    ]
    successful_schedules = [
        run for run in schedule_runs
        if run.get("status") == "completed" and run.get("conclusion") == "success"
    ]
    last_schedule_age_hours = None
    if successful_schedules:
        last_success = max(
            successful_schedules,
            key=lambda run: run.get("run_started_at") or run.get("created_at") or "",
        )
        age = _age_seconds(
            last_success.get("run_started_at") or last_success.get("created_at"),
            now,
        )
        if age is not None:
            last_schedule_age_hours = round(age / 3600, 2)

    if not schedule_runs:
        schedule_state = "bootstrap_pending"
    else:
        latest_schedule = max(
            schedule_runs,
            key=lambda run: run.get("created_at") or run.get("run_started_at") or "",
        )
        latest_status = latest_schedule.get("status")
        if latest_status in {"queued", "waiting", "pending", "requested"}:
            schedule_state = "pending"
        elif latest_status == "in_progress":
            schedule_state = "running"
        elif latest_status == "completed" and latest_schedule.get("conclusion") == "success":
            age = _age_seconds(
                latest_schedule.get("run_started_at") or latest_schedule.get("created_at"),
                now,
            )
            if age is None:
                schedule_state = "unknown"
            else:
                schedule_state = (
                    "stale" if age > MAX_SCHEDULE_AGE_HOURS * 3600 else "fresh"
                )
        else:
            schedule_state = "failed"

        # A new pending/running attempt must not mask an already stale last success.
        if schedule_state in {"pending", "running"}:
            if last_schedule_age_hours is None:
                schedule_state = "bootstrap_pending"
            elif last_schedule_age_hours > MAX_SCHEDULE_AGE_HOURS:
                schedule_state = "stale"

    degraded = (
        stale_queued > 0
        or stale_running > 0
        or stale_jobs > 0
        or stale_queued_jobs > 0
        or schedule_state in {"stale", "unknown", "failed"}
        or bool(missing_workflows)
        or history_truncated
    )
    if degraded:
        status = "degraded"
    elif schedule_state == "bootstrap_pending":
        status = "bootstrap_pending"
    else:
        status = "healthy"

    return {
        "status": status,
        "inspected_infrastructure_runs": len(relevant),
        "stale_queued_runs": stale_queued,
        "stale_running_runs": stale_running,
        "stale_running_jobs": stale_jobs,
        "stale_queued_jobs": stale_queued_jobs,
        "schedule_freshness": schedule_state,
        "last_successful_schedule_age_hours": last_schedule_age_hours,
        "missing_infrastructure_workflows": len(missing_workflows),
        "history_truncated": history_truncated,
    }


def github_get_json(url: str, token: str) -> dict:
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "marketsage-infra-health-check",
        },
        method="GET",
    )
    for attempt in range(2):
        try:
            with urlopen(request, timeout=10) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, OSError, ValueError):
            if attempt == 1:
                raise RuntimeError("github_api_unavailable") from None
            time.sleep(1)
    raise RuntimeError("github_api_unavailable")


def main() -> int:
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GITHUB_TOKEN", "")
    current_run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo) or not token or not current_run_id:
        print(json.dumps({"status": "error", "reason": "missing_context"}))
        return 2

    base_url = f"https://api.github.com/repos/{repo}"
    try:
        runs, missing_workflows, history_truncated = collect_infrastructure_runs(
            base_url, token
        )

        active_runs: list[dict] = []
        for workflow_name in INFRA_NAMES:
            workflow_active = sorted(
                (
                    run for run in runs
                    if run.get("name") == workflow_name
                    and run.get("status") == "in_progress"
                    and str(run.get("id", "")) != current_run_id
                ),
                key=lambda run: run.get("created_at", ""),
                reverse=True,
            )
            active_runs.extend(workflow_active[:MAX_ACTIVE_RUNS_TO_INSPECT])

        jobs_by_run: dict[str, list[dict]] = {}
        for run in active_runs:
            run_id = str(run.get("id", ""))
            jobs_payload = github_get_json(
                f"{base_url}/actions/runs/{run_id}/jobs?per_page=100", token
            )
            jobs = jobs_payload.get("jobs", [])
            jobs_by_run[run_id] = jobs if isinstance(jobs, list) else []

        report = assess_health(
            runs,
            jobs_by_run,
            current_run_id,
            datetime.now(timezone.utc),
            missing_workflows,
            history_truncated,
        )
    except Exception:
        print(json.dumps({"status": "error", "reason": "github_api_unavailable"}))
        return 2

    print(json.dumps(report, sort_keys=True))
    return 1 if report["status"] == "degraded" else 0


if __name__ == "__main__":
    sys.exit(main())
