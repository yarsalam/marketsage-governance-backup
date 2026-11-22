import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.automation.health_check import (
    CI_NAME,
    WATCHDOG_NAME,
    MAX_RUN_PAGES,
    PAGE_SIZE,
    assess_health,
    collect_infrastructure_runs,
)


NOW = datetime(2026, 11, 22, 7, 0, tzinfo=timezone.utc)


def stamp(delta: timedelta) -> str:
    return (NOW - delta).isoformat().replace("+00:00", "Z")


def make_run(
    run_id: int,
    name: str = CI_NAME,
    event: str = "push",
    status: str = "completed",
    conclusion: str = "success",
    age: timedelta = timedelta(minutes=2),
) -> dict:
    return {
        "id": run_id,
        "name": name,
        "event": event,
        "status": status,
        "conclusion": conclusion,
        "created_at": stamp(age),
        "run_started_at": stamp(age),
    }


class HealthAssessmentTests(unittest.TestCase):
    def test_first_schedule_is_bootstrap_not_failure(self):
        result = assess_health([], {}, "100", NOW)
        self.assertEqual(result["status"], "bootstrap_pending")
        self.assertEqual(result["schedule_freshness"], "bootstrap_pending")

    def test_first_schedule_running_is_bootstrap_not_healthy(self):
        run = make_run(
            10,
            name=WATCHDOG_NAME,
            event="schedule",
            status="in_progress",
            conclusion="",
            age=timedelta(minutes=1),
        )
        result = assess_health([run], {}, "100", NOW)
        self.assertEqual(result["status"], "bootstrap_pending")
        self.assertEqual(result["schedule_freshness"], "bootstrap_pending")

    def test_recent_successful_schedule_is_healthy(self):
        run = make_run(
            10,
            name=WATCHDOG_NAME,
            event="schedule",
            age=timedelta(hours=6),
        )
        result = assess_health([run], {}, "100", NOW)
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["schedule_freshness"], "fresh")

    def test_old_successful_schedule_is_degraded(self):
        run = make_run(
            10,
            name=WATCHDOG_NAME,
            event="schedule",
            age=timedelta(hours=9),
        )
        result = assess_health([run], {}, "100", NOW)
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["schedule_freshness"], "stale")

    def test_pending_schedule_does_not_mask_old_success(self):
        success = make_run(
            20,
            name=WATCHDOG_NAME,
            event="schedule",
            age=timedelta(hours=9),
        )
        pending = make_run(
            21,
            name=WATCHDOG_NAME,
            event="schedule",
            status="queued",
            conclusion="",
            age=timedelta(minutes=1),
        )
        result = assess_health([success, pending], {}, "100", NOW)
        self.assertEqual(result["schedule_freshness"], "stale")
        self.assertEqual(result["status"], "degraded")

    def test_old_queued_infrastructure_run_is_detected(self):
        run = make_run(
            11,
            status="queued",
            conclusion="",
            age=timedelta(minutes=25),
        )
        result = assess_health([run], {}, "100", NOW)
        self.assertEqual(result["stale_queued_runs"], 1)
        self.assertEqual(result["status"], "degraded")

    def test_old_running_job_is_detected(self):
        run = make_run(
            12,
            status="in_progress",
            conclusion="",
            age=timedelta(minutes=5),
        )
        jobs = {
            "12": [{
                "status": "in_progress",
                "started_at": stamp(timedelta(minutes=13)),
            }]
        }
        result = assess_health([run], jobs, "100", NOW)
        self.assertEqual(result["stale_running_jobs"], 1)
        self.assertEqual(result["status"], "degraded")

    def test_failed_latest_schedule_is_degraded_even_if_previous_success_is_fresh(self):
        success = make_run(
            20,
            name=WATCHDOG_NAME,
            event="schedule",
            age=timedelta(hours=6),
        )
        failed = make_run(
            21,
            name=WATCHDOG_NAME,
            event="schedule",
            conclusion="failure",
            age=timedelta(minutes=10),
        )
        result = assess_health([success, failed], {}, "100", NOW)
        self.assertEqual(result["schedule_freshness"], "failed")
        self.assertEqual(result["status"], "degraded")

    def test_queued_job_is_detected_even_when_run_started_recently(self):
        run = make_run(
            22,
            status="in_progress",
            conclusion="",
            age=timedelta(minutes=5),
        )
        run["created_at"] = stamp(timedelta(minutes=30))
        jobs = {
            "22": [{
                "status": "queued",
                "started_at": None,
            }]
        }
        result = assess_health([run], jobs, "100", NOW)
        self.assertEqual(result["stale_queued_jobs"], 1)
        self.assertEqual(result["status"], "degraded")

    def test_current_watchdog_run_is_excluded(self):
        run = make_run(
            100,
            name=WATCHDOG_NAME,
            event="schedule",
            status="in_progress",
            conclusion="",
            age=timedelta(hours=30),
        )
        result = assess_health([run], {}, "100", NOW)
        self.assertEqual(result["status"], "bootstrap_pending")
        self.assertEqual(result["inspected_infrastructure_runs"], 0)

    def test_unrelated_workflows_are_ignored(self):
        run = make_run(
            13,
            name="Unrelated Workflow",
            status="queued",
            conclusion="",
            age=timedelta(days=2),
        )
        result = assess_health([run], {}, "100", NOW)
        self.assertEqual(result["status"], "bootstrap_pending")
        self.assertEqual(result["stale_queued_runs"], 0)

    def test_missing_workflow_is_degraded(self):
        result = assess_health([], {}, "100", NOW, missing_workflows=[CI_NAME])
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["missing_infrastructure_workflows"], 1)

    def test_truncated_history_is_degraded(self):
        result = assess_health([], {}, "100", NOW, history_truncated=True)
        self.assertEqual(result["status"], "degraded")
        self.assertTrue(result["history_truncated"])


class WorkflowRunCollectionTests(unittest.TestCase):
    def test_collects_runs_by_workflow_id(self):
        base_url = "https://api.github.com/repos/yarsalam/marketsage"

        def fake_get(url: str, token: str) -> dict:
            if url.endswith("/actions/workflows?per_page=100"):
                return {
                    "total_count": 2,
                    "workflows": [
                        {"id": 101, "name": CI_NAME, "state": "active"},
                        {"id": 202, "name": WATCHDOG_NAME, "state": "active"},
                    ],
                }
            if "/actions/workflows/101/runs?" in url:
                return {"workflow_runs": [make_run(1, name=CI_NAME)]}
            if "/actions/workflows/202/runs?" in url:
                return {"workflow_runs": [make_run(2, name=WATCHDOG_NAME, event="schedule")]}
            self.fail("Unexpected API endpoint")

        with patch("scripts.automation.health_check.github_get_json", side_effect=fake_get):
            runs, missing, truncated = collect_infrastructure_runs(base_url, "test-token")

        self.assertEqual({run["id"] for run in runs}, {1, 2})
        self.assertEqual(missing, [])
        self.assertFalse(truncated)

    def test_bounded_pagination_marks_incomplete_history(self):
        base_url = "https://api.github.com/repos/yarsalam/marketsage"

        def fake_get(url: str, token: str) -> dict:
            if url.endswith("/actions/workflows?per_page=100"):
                return {
                    "total_count": 2,
                    "workflows": [
                        {"id": 101, "name": CI_NAME, "state": "active"},
                        {"id": 202, "name": WATCHDOG_NAME, "state": "active"},
                    ],
                }
            if "/actions/workflows/101/runs?" in url:
                return {"workflow_runs": [make_run(i, name=CI_NAME) for i in range(PAGE_SIZE)]}
            if "/actions/workflows/202/runs?" in url:
                return {"workflow_runs": []}
            self.fail("Unexpected API endpoint")

        with patch("scripts.automation.health_check.github_get_json", side_effect=fake_get):
            runs, missing, truncated = collect_infrastructure_runs(base_url, "test-token")

        self.assertEqual(len(runs), PAGE_SIZE * MAX_RUN_PAGES)
        self.assertEqual(missing, [])
        self.assertTrue(truncated)


if __name__ == "__main__":
    unittest.main()
