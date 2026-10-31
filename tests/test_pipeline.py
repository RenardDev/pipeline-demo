"""Check the orchestration and evidence paths without waiting five minutes."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import demo_pipeline as demo


class PipelineTests(unittest.TestCase):
    def run_local(self, directory, fail_at="none"):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "demo_pipeline.py"), "local",
             "--duration", "0.3", "--fail-at", fail_at, "--reports-dir", str(directory)],
            cwd=ROOT, capture_output=True, encoding="utf-8", timeout=30,
        )
        self.assertTrue((directory / "report.html").is_file(), result.stderr)
        payload = json.loads((directory / "report.json").read_text(encoding="utf-8"))
        return result, payload, {node["id"]: node for node in payload["nodes"]}

    def test_success_has_all_nodes_and_expected_skipped_rollback(self):
        with tempfile.TemporaryDirectory() as folder:
            result, payload, nodes = self.run_local(Path(folder))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(nodes), 32)
        self.assertEqual(payload["status"], "success")
        self.assertEqual(nodes["rollback"]["status"], "skipped")
        self.assertTrue(all(n["status"] == "success" for key, n in nodes.items() if key != "rollback"))
        for node in nodes.values():
            if node["status"] == "success" and node["id"] != "report":
                self.assertEqual(len(node["tasks"]), 3)
                self.assertTrue(node["events"])
                for parent in node["needs"]:
                    if nodes[parent]["finished_at"]:
                        self.assertGreaterEqual(node["started_at"], nodes[parent]["finished_at"])

    def test_canary_failure_runs_rollback_and_preserves_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            result, payload, nodes = self.run_local(Path(folder), "canary")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(payload["status"], "failure")
        self.assertEqual(nodes["canary"]["status"], "failure")
        self.assertEqual(nodes["rollback"]["status"], "success")
        self.assertEqual(nodes["promote"]["status"], "skipped")
        self.assertEqual(nodes["cleanup"]["status"], "success")
        self.assertEqual(nodes["report"]["status"], "failure")

    def test_early_failure_skips_dependents_but_keeps_independent_lanes(self):
        for failed in ("unit_api", "security_gate", "e2e_chromium"):
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as folder:
                result, _, nodes = self.run_local(Path(folder), failed)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(nodes[failed]["status"], "failure")
                self.assertEqual(nodes["quality_gate"]["status"], "skipped")
                self.assertEqual(nodes["preview"]["status"], "skipped")
                self.assertEqual(nodes["e2e_firefox"]["status"], "success")
                self.assertEqual(nodes["rollback"]["status"], "skipped")
                self.assertEqual(nodes["cleanup"]["status"], "success")

    def test_separate_github_steps_keep_failure_and_export_shared_clock(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            started = str(time.time() - 600)  # An expired deadline must not add waits.
            environment = dict(os.environ, DEMO_STARTED_AT=started, DEMO_DURATION="300",
                               DEMO_FAILURE="unit_api", DEMO_REPORTS_DIR=str(directory),
                               GITHUB_OUTPUT=str(directory / "output"),
                               GITHUB_STEP_SUMMARY=str(directory / "summary"))
            codes = []
            for part in ("prepare", "work", "finish"):
                result = subprocess.run(
                    [sys.executable, str(ROOT / "scripts" / "demo_pipeline.py"), "stage",
                     "--node", "unit_api", "--part", part],
                    cwd=ROOT, env=environment, capture_output=True, encoding="utf-8", timeout=10,
                )
                codes.append(result.returncode)
            record = json.loads((directory / "unit_api.json").read_text(encoding="utf-8"))
            self.assertEqual(codes, [0, 1, 1])
            self.assertEqual(record["status"], "failure")
            self.assertIn("FAILURE", (directory / "summary").read_text(encoding="utf-8"))
            output = (directory / "output").read_text(encoding="utf-8")
            self.assertAlmostEqual(float(output.split("=", 1)[1]), float(started), places=4)

    def test_missing_artifacts_do_not_become_a_successful_report(self):
        data = demo.config()
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            ctx = demo.Context(time.time() - 600, 300, "none", directory)
            actual = {node["id"]: {"result": "success"} for node in data["nodes"] if node["id"] != "report"}
            actual["rollback"] = {"result": "skipped"}
            with contextlib.redirect_stdout(io.StringIO()):
                result = demo.report(data, ctx, actual)
            payload = json.loads((directory / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(result, 1)
        self.assertEqual(payload["status"], "failure")
        self.assertTrue(any("missing" in problem for problem in payload["problems"]))

    def test_nested_artifacts_keep_latest_evidence_for_each_node(self):
        data = demo.config()
        node = data["nodes"][0]
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            ctx = demo.Context(time.time(), 300, "none", directory)
            old = demo.blank_record(node, ctx)
            old.update(status="failure", started_at=ctx.started_at, finished_at=ctx.started_at + 1)
            new = dict(old, status="success", finished_at=ctx.started_at + 2)
            demo.save(directory / "process-plan-1" / "plan.json", old)
            demo.save(directory / "process-plan-2" / "plan.json", new)
            demo.save(directory / "report.json", {"kind": "demo-pipeline-report"})
            records = demo.collect_records(directory, ctx.started_at)
        self.assertEqual(list(records), ["plan"])
        self.assertEqual(records["plan"]["status"], "success")


if __name__ == "__main__":
    unittest.main()
