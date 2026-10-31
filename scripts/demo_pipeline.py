#!/usr/bin/env python3
"""Observable, deliberately simulated GitHub Actions workload (stdlib only)."""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
import html
import json
import math
import os
from pathlib import Path
import sys
import threading
import time


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "demo" / "pipeline.json"
LOCK = threading.RLock()
TERMINAL = {"success", "failure", "cancelled", "skipped", "missing"}
COLORS = {
    "success": "#50e3b5", "failure": "#ff6b86", "cancelled": "#ffb86b",
    "skipped": "#697894", "missing": "#ffb86b", "running": "#7daaff",
}


def utc(value: float | None) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="milliseconds") if value else "—"


def config() -> dict:
    data = json.loads(CONFIG.read_text(encoding="utf-8"))
    nodes = data.get("nodes", [])
    ids = [node["id"] for node in nodes]
    if not nodes or len(set(ids)) != len(ids):
        raise ValueError("pipeline.json must contain nodes with unique IDs")
    seen: set[str] = set()
    by_id = {node["id"]: node for node in nodes}

    def visit(node_id: str, stack: set[str]) -> None:
        if node_id not in by_id:
            raise ValueError(f"Unknown dependency: {node_id}")
        if node_id in stack:
            raise ValueError(f"Dependency cycle involving {node_id}")
        if node_id in seen:
            return
        for parent in by_id[node_id].get("needs", []):
            visit(parent, stack | {node_id})
        seen.add(node_id)

    for node in nodes:
        visit(node["id"], set())
        if not 0 <= float(node["offset"]) <= float(data.get("target_seconds", 300)):
            raise ValueError(f"Invalid deadline for {node['id']}")
    return data


@dataclass(frozen=True)
class Context:
    started_at: float
    duration: float
    failure: str
    reports: Path
    baseline: float = 300.0
    summary: str | None = None
    output: str | None = None
    local: bool = False
    run_attempt: str = "local"

    def deadline(self, node: dict) -> float:
        return self.started_at + float(node["offset"]) * self.duration / self.baseline


def environment_context(data: dict) -> Context:
    duration = float(os.environ.get("DEMO_DURATION", data.get("target_seconds", 300)))
    if duration <= 0:
        raise ValueError("DEMO_DURATION must be positive")
    reports = Path(os.environ.get("DEMO_REPORTS_DIR", "reports")).resolve()
    raw_start = os.environ.get("DEMO_STARTED_AT", "").strip()
    if raw_start:
        started = float(raw_start)
    else:
        previous = collect_records(reports)
        started = min((r.get("run_started_at", time.time()) for r in previous.values()), default=time.time())
    return Context(started, duration, os.environ.get("DEMO_FAILURE", "none"), reports,
                   float(data.get("target_seconds", 300)), os.environ.get("GITHUB_STEP_SUMMARY"),
                   os.environ.get("GITHUB_OUTPUT"), run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT", "local"))


def path_for(ctx: Context, node_id: str) -> Path:
    # report.json is the aggregate artifact; keep the report node's telemetry separate.
    return ctx.reports / ("_report.json" if node_id == "report" else f"{node_id}.json")


def save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if data.get("kind") == "demo-node":
        data["recorded_at"] = time.time()
    temporary = path.with_name(f".{path.name}.{threading.get_ident()}.tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for attempt in range(6):
        try:
            temporary.replace(path)
            break
        except PermissionError:
            # Windows indexers can briefly hold a freshly written artifact open.
            if attempt == 5:
                raise
            time.sleep(.015 * (attempt + 1))


def append(path: str | None, value: str) -> None:
    if path:
        with LOCK, open(path, "a", encoding="utf-8") as handle:
            handle.write(value)


def emit(message: str, level: str = "info") -> None:
    color = {"info": "36", "success": "32", "warning": "33", "error": "31"}.get(level, "36")
    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    with LOCK:
        print(f"\033[{color}m[{stamp} UTC]\033[0m {message}", flush=True)


def event(record: dict, part: str, message: str, level: str = "info", **extra: object) -> None:
    record.setdefault("events", []).append({"at": time.time(), "part": part, "message": message,
                                            "level": level, **extra})
    emit(f"[{record['id']}/{part}] {message}", level)


def blank_record(node: dict, ctx: Context) -> dict:
    return {"kind": "demo-node", "schema_version": 1, "id": node["id"], "name": node["name"],
            "stage": node["stage"], "needs": node.get("needs", []), "status": "pending",
            "run_started_at": ctx.started_at, "target_seconds": ctx.duration,
            "github_run_attempt": ctx.run_attempt,
            "deadline_at": ctx.deadline(node), "started_at": None, "finished_at": None,
            "metric": node.get("metric", "Simulated workload"), "simulated": True,
            "tasks": [{"name": task, "status": "pending", "started_at": None, "finished_at": None}
                      for task in node.get("tasks", [])], "events": []}


def load_record(node: dict, ctx: Context) -> dict:
    path = path_for(ctx, node["id"])
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else blank_record(node, ctx)


def progress(record: dict, ctx: Context, part: str, seconds: float, label: str) -> None:
    """Use a monotonic timer for sleeps; timestamps remain real wall-clock measurements."""
    seconds = max(0.0, seconds)
    count = max(3, int(seconds / 2) + 1)
    start = time.monotonic()
    for index in range(count):
        target = start + seconds * (index + 1) / count
        time.sleep(max(0.0, target - time.monotonic()))
        percent = round(100 * (index + 1) / count)
        filled = round(percent / 10)
        event(record, part, f"{'█' * filled}{'░' * (10 - filled)} {percent:3d}% · {label}", progress=percent)
        save(path_for(ctx, record["id"]), record)


def node_summary(record: dict) -> str:
    elapsed = max(0.0, (record.get("finished_at") or time.time()) - (record.get("started_at") or time.time()))
    lines = [f"### {record['name']}", "", f"**{record['status'].upper()}** · `{record['id']}` · {elapsed:.2f}s",
             "", "> Demonstration only. All task results and quality metrics are simulated.", "",
             "| Process | Result |", "| :-- | :-- |"]
    lines += [f"| {task['name'].replace('|', '/')} | {task['status']} |" for task in record.get("tasks", [])]
    lines += ["", f"Simulated metric: {record.get('metric', '—')}", ""]
    return "\n".join(lines) + "\n"


def stage(node: dict, part: str, ctx: Context) -> int:
    record = load_record(node, ctx)
    path = path_for(ctx, node["id"])
    with LOCK:
        print(f"::group::{node['name']} · {part.upper()} · SIMULATION", flush=True)
    try:
        if part == "prepare":
            record["started_at"] = record.get("started_at") or time.time()
            record["status"] = "running"
            append(ctx.output, f"started_at={ctx.started_at:.6f}\n")
            event(record, part, f"Preparing simulated {node['stage']} workload; deadline {utc(ctx.deadline(node))}")
            event(record, part, f"Dependencies: {', '.join(node.get('needs', [])) or 'entry point'}; no external services are contacted")
            save(path, record)
            progress(record, ctx, part, max(0.0, ctx.deadline(node) - time.time()) * .15, "allocate demo resources")
        elif part == "work":
            if record["status"] == "failure":
                return 1
            record["started_at"] = record.get("started_at") or time.time()
            record["status"] = "running"
            budget = max(0.0, ctx.deadline(node) - time.time()) * .75
            tasks = record.get("tasks", [])
            for index, task in enumerate(tasks):
                task["started_at"] = time.time()
                task["status"] = "running"
                event(record, part, f"Process {index + 1}/{len(tasks)}: {task['name']} (simulated)")
                save(path, record)
                progress(record, ctx, part, budget / max(1, len(tasks)), task["name"])
                task["finished_at"] = time.time()
                failing = ctx.failure == node["id"] and index == len(tasks) - 1
                task["status"] = "failure" if failing else "success"
                event(record, part, f"{task['name']}: {'injected demo failure' if failing else 'simulated result OK'}",
                      "error" if failing else "success")
            if not tasks:
                progress(record, ctx, part, budget, "simulated process")
            if ctx.failure == node["id"]:
                record["status"] = "failure"
                record["error"] = f"Intentional demo failure requested for {node['id']}"
                event(record, part, record["error"], "error")
                with LOCK:
                    print(f"::error title=Injected simulation failure::{node['id']}: intentional failure for DAG demonstration", flush=True)
                save(path, record)
                return 1
            event(record, part, f"SIMULATED METRIC · {record['metric']}", "success")
        elif part == "finish":
            event(record, part, "Collecting telemetry, process timings, and simulated quality results")
            progress(record, ctx, part, max(0.0, ctx.deadline(node) - time.time()), "finalize node telemetry")
            if record["status"] not in {"failure", "cancelled"}:
                record["status"] = "success"
            record["finished_at"] = time.time()
            event(record, part, f"Node completed: {record['status']}", "error" if record["status"] == "failure" else "success")
            append(ctx.summary, node_summary(record))
        else:
            raise ValueError(f"Unknown stage part: {part}")
        save(path, record)
        return 1 if record["status"] == "failure" else 0
    except Exception as exc:
        record["status"] = "failure"
        record["finished_at"] = time.time()
        record["error"] = str(exc)
        save(path, record)
        raise
    finally:
        with LOCK:
            print("::endgroup::", flush=True)


def collect_records(directory: Path, run_started_at: float | None = None) -> dict[str, dict]:
    records: dict[str, dict] = {}
    if not directory.exists():
        return records
    for path in sorted(directory.rglob("*.json")):
        try:
            item = json.loads(path.read_text(encoding="utf-8-sig"))
        except (ValueError, OSError):
            continue
        if not isinstance(item, dict) or item.get("kind") != "demo-node" or not isinstance(item.get("id"), str):
            continue
        timing_fields = ("run_started_at", "started_at", "finished_at", "recorded_at", "deadline_at")
        if any(item.get(key) is not None and (not isinstance(item[key], (int, float)) or not math.isfinite(item[key])) for key in timing_fields):
            continue
        if run_started_at is not None and abs((item.get("run_started_at") or 0) - run_started_at) > .01:
            continue
        previous = records.get(item["id"], {})

        def freshness(record: dict) -> tuple[int, float]:
            try:
                attempt = int(record.get("github_run_attempt", 0))
            except (TypeError, ValueError):
                attempt = 0
            return attempt, record.get("finished_at") or record.get("started_at") or record.get("recorded_at") or 0

        if freshness(item) >= freshness(previous):
            records[item["id"]] = item
    return records


def resolve_records(data: dict, ctx: Context, actual: dict) -> list[dict]:
    collected = collect_records(ctx.reports, ctx.started_at)
    resolved: dict[str, dict] = {}
    pending = [node for node in data["nodes"] if node["id"] != "report"]
    while pending:
        for node in list(pending):
            if any(parent not in resolved for parent in node.get("needs", []) if parent != "report"):
                continue
            node_id = node["id"]
            record = {**blank_record(node, ctx), **collected.get(node_id, {})}
            record["telemetry_missing"] = node_id not in collected
            evidence = actual.get(node_id, {})
            actual_status = evidence.get("result") if isinstance(evidence, dict) else evidence
            if isinstance(actual_status, str) and actual_status in TERMINAL:
                record["status"] = actual_status
                record["status_source"] = "Local DAG scheduler" if ctx.local else "GitHub Actions"
            elif record["status"] not in TERMINAL:
                upstream_blocked = any(resolved[parent]["status"] != "success" for parent in node.get("needs", []) if parent in resolved)
                if node_id == "rollback" and resolved.get("canary", {}).get("status") != "failure":
                    record["status"] = "skipped"
                    record["skip_reason"] = "Canary did not fail; rollback was not needed"
                elif upstream_blocked and node_id not in {"rollback", "cleanup"}:
                    record["status"] = "skipped"
                    record["skip_reason"] = "A dependency did not succeed"
                else:
                    record["status"] = "missing"
            record["simulated"] = True
            resolved[node_id] = record
            pending.remove(node)
    return [resolved[node["id"]] for node in data["nodes"] if node["id"] != "report"]


def escape(value: object) -> str:
    return html.escape(str(value), quote=True)


def elapsed(record: dict) -> float:
    if record.get("started_at") and record.get("finished_at"):
        return max(0.0, record["finished_at"] - record["started_at"])
    return 0.0


def markdown_report(payload: dict) -> str:
    nodes = payload["nodes"]
    lines = ["# Pipeline observatory", "", "> DEMONSTRATION: all workloads, deployments, checks, and quality metrics are simulated.", "",
             f"**Result: {payload['status'].upper()}** · Recorded duration: **{payload['elapsed_seconds']:.2f}s** · Target: {payload['target_seconds']:g}s", "",
             f"Download the `{payload['artifact_name']}` artifact and open `report.html` for the interactive DAG, timeline, process details, and timestamped logs.", "",
             "| Node | Stage | Result | Real elapsed | Simulated metric |", "| :-- | :-- | :-- | --: | :-- |"]
    for record in nodes:
        metric = record.get("metric", "—").replace("|", "/").replace("\n", " ")
        name = record["name"].replace("|", "/")
        suffix = " (telemetry missing)" if record.get("telemetry_missing") and record["status"] != "skipped" else ""
        lines.append(f"| {name} | {record['stage']} | {record['status']}{suffix} | {elapsed(record):.2f}s | {metric} |")
    lines += ["", "## Dependency graph", "", "```mermaid", "flowchart LR"]
    for record in nodes:
        name = record["name"].replace('"', "'")
        lines.append(f'  {record["id"]}["{name} · {record["status"]}"]')
        lines += [f"  {parent} --> {record['id']}" for parent in record.get("needs", [])]
    lines += ["```", "", "## Timing", "", f"- Started (UTC): {utc(payload['started_at'])}",
              f"- Finished (UTC): {utc(payload['finished_at'])}",
              "- Node durations and event timestamps are measured from actual execution.",
              "- Hosted runner allocation, queueing, and artifact transfers can extend wall-clock runtime beyond the target.", ""]
    if payload.get("run_url"):
        lines += [f"[Open this GitHub Actions run]({payload['run_url']})", ""]
    if payload["problems"]:
        lines += ["## Attention required", ""] + [f"- {problem}" for problem in payload["problems"]] + [""]
    return "\n".join(lines)


def graph_svg(nodes: list[dict]) -> str:
    by_id = {node["id"]: node for node in nodes}
    levels: dict[str, int] = {}

    def level(node_id: str) -> int:
        if node_id not in levels:
            levels[node_id] = 1 + max((level(parent) for parent in by_id[node_id].get("needs", []) if parent in by_id), default=-1)
        return levels[node_id]

    columns: dict[int, list[dict]] = {}
    for node in nodes:
        columns.setdefault(level(node["id"]), []).append(node)
    width = max(900, len(columns) * 238 + 46)
    height = max(235, max(map(len, columns.values()), default=1) * 92 + 70)
    points: dict[str, tuple[float, float]] = {}
    headers = []
    for column, group in columns.items():
        labels = list(dict.fromkeys(node["stage"] for node in group))
        headers.append(f'<text x="{30 + column * 238}" y="28" class="column-title">{escape(" / ".join(labels)).upper()}</text>')
        for row, node in enumerate(group):
            points[node["id"]] = (30 + column * 238, 48 + row * 92)
    edges = []
    boxes = []
    for node in nodes:
        x, y = points[node["id"]]
        color = COLORS.get(node["status"], COLORS["missing"])
        for parent in node.get("needs", []):
            if parent not in points:
                continue
            px, py = points[parent]
            source = COLORS.get(by_id[parent]["status"], "#697894")
            bend = (px + 194 + x) / 2
            edges.append(f'<path d="M{px + 194},{py + 34} C{bend},{py + 34} {bend},{y + 34} {x},{y + 34}" stroke="{source}" class="edge" marker-end="url(#arrow)"/>')
        label = node["name"]
        if len(label) > 24:
            label = label[:22] + "…"
        boxes.append(f'<g class="graph-node" data-node="{escape(node["id"])}" role="button" tabindex="0" aria-label="{escape(node["name"] + ": " + node["status"])}"><title>{escape(node["name"])}</title>'
                     f'<rect x="{x}" y="{y}" width="194" height="68" rx="12" fill="#142139" stroke="{color}" stroke-opacity=".65"/>'
                     f'<circle cx="{x + 16}" cy="{y + 19}" r="4" fill="{color}"/>'
                     f'<text x="{x + 28}" y="{y + 23}" class="node-name">{escape(label)}</text>'
                     f'<text x="{x + 15}" y="{y + 49}" class="node-meta" fill="{color}">{escape(node["status"].upper())} · {elapsed(node):.1f}s</text></g>')
    return f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" aria-label="Pipeline dependency graph"><defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10z" fill="#536680"/></marker></defs>{"".join(headers + edges + boxes)}</svg>'


def html_report(payload: dict) -> str:
    nodes = payload["nodes"]
    counts = {status: sum(node["status"] == status for node in nodes) for status in COLORS}
    cards = "".join(f'<div class="stat"><strong style="color:{COLORS[status]}">{counts[status]:02}</strong><span>{status}</span></div>' for status in ("success", "failure", "skipped", "missing"))
    timeline = []
    span = max(payload["elapsed_seconds"], payload["target_seconds"], .01)
    for record in nodes:
        started = record.get("started_at")
        left = max(0, (started - payload["started_at"]) / span * 100) if started else 0
        length = max(.7, min(100 - left, elapsed(record) / span * 100))
        color = COLORS.get(record["status"], "#697894")
        bar = f'<span class="bar" style="left:{left:.3f}%;width:{length:.3f}%;background:{color}"></span>' if started else '<span class="no-run">did not run</span>'
        timeline.append(f'<button class="timeline-row" data-node="{escape(record["id"])}"><span class="timeline-name">{escape(record["name"])}</span><span class="track">{bar}</span><span class="duration">{elapsed(record):.1f}s</span></button>')
    embedded = json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    template = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pipeline observatory · Demo run</title>
<style>
:root{color-scheme:dark;--bg:#080e1b;--panel:#0f192c;--line:#22314a;--muted:#8c9db8;--text:#e9f1ff;--accent:#50e3b5}*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at 85% -10%,#183152 0,transparent 50%),var(--bg);font:14px/1.5 system-ui,sans-serif;color:var(--text)}main{max-width:1680px;margin:auto;padding:40px 32px 70px}header{display:flex;justify-content:space-between;gap:25px;align-items:center}.eyebrow{color:var(--accent);font-size:11px;letter-spacing:.19em;font-weight:700}h1{font-size:clamp(30px,4vw,48px);line-height:1.1;letter-spacing:-.04em;margin:12px 0}h2{font-size:18px;margin:0}p{color:var(--muted)}.pill{border:1px solid #315344;background:#102b29;color:var(--accent);padding:9px 14px;border-radius:25px;font-size:12px;font-weight:700;white-space:nowrap}.notice{border-left:3px solid #7daaff;background:#102039;padding:12px 16px;margin:26px 0;color:#b8c9e5}.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin:24px 0}.stat{border:1px solid var(--line);background:var(--panel);border-radius:14px;padding:18px 20px;display:flex;flex-direction:column}.stat strong{font-size:32px;font-variant-numeric:tabular-nums;line-height:1.2}.stat span{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.1em;margin-top:7px}.panel{border:1px solid var(--line);background:rgba(15,25,44,.95);border-radius:18px;margin:20px 0;overflow:hidden}.panel-head{padding:20px 24px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;gap:16px;align-items:center}.panel-head p{margin:4px 0 0;font-size:12px}.legend{display:flex;gap:16px;color:var(--muted);font-size:11px;flex-wrap:wrap}.legend i{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px}.graph-scroll{overflow:auto;padding:10px 0 20px}.column-title{fill:#8c9db8;font:10px system-ui;letter-spacing:.07em}.node-name{fill:#e9f1ff;font:12px system-ui;font-weight:600}.node-meta{font:10px ui-monospace,monospace}.edge{fill:none;stroke-width:1.7;opacity:.55}.graph-node{cursor:pointer;outline:none}.graph-node:hover rect,.graph-node:focus rect,.graph-node.selected rect{stroke:#fff;stroke-width:2;filter:drop-shadow(0 0 8px #7daaff33)}.columns{display:grid;grid-template-columns:minmax(0,1.1fr) minmax(0,1fr);gap:20px;align-items:start}.columns .panel{margin:0}.timeline{padding:16px 20px 22px}.ruler{margin:0 44px 8px 182px;display:flex;justify-content:space-between;color:var(--muted);font-size:10px;font-family:ui-monospace,monospace}.timeline-row{display:grid;grid-template-columns:172px minmax(70px,1fr) 40px;gap:10px;align-items:center;width:100%;border:0;border-radius:6px;color:inherit;background:none;padding:6px 0;text-align:left;cursor:pointer;font:inherit}.timeline-row:hover,.timeline-row.selected{background:#182640}.timeline-name{overflow:hidden;white-space:nowrap;text-overflow:ellipsis;font-size:11px}.track{position:relative;display:block;height:15px;background:repeating-linear-gradient(90deg,transparent 0,transparent calc(25% - 1px),#22314a 25%);border-left:1px solid #32405a}.bar{height:9px;top:3px;position:absolute;border-radius:3px;opacity:.9}.duration{font:10px ui-monospace,monospace;color:var(--muted);text-align:right}.no-run{font-size:9px;color:#697894;padding-left:6px}.detail{padding:20px 24px}.detail-title{display:flex;justify-content:space-between;align-items:start;gap:10px}.detail-title h3{margin:0;font-size:20px}.detail-title code{font-size:11px;color:var(--muted)}.badge{font-size:10px;text-transform:uppercase;border:1px solid currentColor;border-radius:5px;padding:3px 7px}.meta-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:20px 0}.meta-grid small{display:block;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.07em}.meta-grid span{font-size:12px;word-break:break-word}.metric{border:1px solid #294a4b;background:#10262d;padding:12px;border-radius:10px;color:#9ce5db;font-size:12px}.metric small{display:block;font-size:9px;letter-spacing:.1em;color:#74a8ac;margin-bottom:4px}.tasks{padding:0;list-style:none;margin:20px 0}.tasks li{display:flex;gap:10px;padding:8px 0;border-bottom:1px solid var(--line);font-size:12px}.tasks .dot{width:7px;height:7px;border-radius:50%;margin-top:6px;flex:none}.tasks span:last-child{margin-left:auto;color:var(--muted);font-size:10px}.logs{border:1px solid var(--line);background:#090f1c;border-radius:10px;padding:12px;max-height:320px;overflow:auto;font:10px/1.75 ui-monospace,Consolas,monospace;white-space:pre-wrap;overflow-wrap:anywhere}.error-list{color:#ffb4c0}.footer{display:flex;justify-content:space-between;color:#7184a2;font:11px ui-monospace,monospace;margin-top:26px;gap:20px}button:focus-visible{outline:2px solid #7daaff;outline-offset:2px}@media(max-width:1000px){.columns{grid-template-columns:1fr}.stats{grid-template-columns:repeat(3,1fr)}}@media(max-width:600px){main{padding:24px 14px}.stats{gap:7px}.stat{padding:13px}.stat strong{font-size:26px}header{align-items:start;flex-direction:column}.panel-head{align-items:start;flex-direction:column}.timeline-row{grid-template-columns:115px minmax(40px,1fr) 35px;gap:6px}.ruler{margin-left:121px;margin-right:41px}.timeline-name{font-size:10px}.detail{padding:18px}.footer{flex-direction:column}}
</style></head><body><main>
<header><div><div class="eyebrow">DEMO / WORKFLOW OBSERVATORY</div><h1>Every process, in view.</h1><p>A recorded execution map of parallel jobs, quality gates, and recovery paths.</p></div><div class="pill">@@STATUS@@ · SIMULATION</div></header>
<div class="notice"><strong>Simulated workload.</strong> Builds, tests, scans, deployments, and metrics are synthetic. Dependencies, job results, timestamps, failure propagation, and durations are real execution records.</div>
<section class="stats">@@CARDS@@<div class="stat"><strong>@@ELAPSED@@<small style="font-size:14px"> s</small></strong><span>recorded wall time</span></div><div class="stat"><strong>@@TARGET@@<small style="font-size:14px"> s</small></strong><span>target duration</span></div></section>
<section class="panel"><div class="panel-head"><div><h2>Dependency map</h2><p>Click a node to inspect its processes and timestamped logs. Scroll horizontally to explore every connection.</p></div><div class="legend"><span><i style="background:#50e3b5"></i>Success</span><span><i style="background:#ff6b86"></i>Failure</span><span><i style="background:#697894"></i>Skipped</span></div></div><div class="graph-scroll">@@GRAPH@@</div></section>
<div class="columns"><section class="panel"><div class="panel-head"><div><h2>Execution timeline</h2><p>Measured start and finish times, including simulated work delays.</p></div></div><div class="timeline"><div class="ruler"><span>0s</span><span>@@HALF@@s</span><span>@@SPAN@@s</span></div>@@TIMELINE@@</div></section><section class="panel"><div class="panel-head"><div><h2>Process inspector</h2><p>Full task history for the selected node.</p></div></div><div class="detail" id="detail"></div></section></div>
<div id="problems"></div><footer class="footer"><span>@@STARTED@@ UTC</span><span>Self-contained report · No external resources · All quality metrics simulated</span></footer>
</main><script type="application/json" id="run-data">@@DATA@@</script><script>
const run=JSON.parse(document.getElementById('run-data').textContent);
const colors={success:'#50e3b5',failure:'#ff6b86',skipped:'#697894',cancelled:'#ffb86b',missing:'#ffb86b',running:'#7daaff',pending:'#697894'};
const esc=value=>String(value??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const relative=t=>t?Math.max(0,t-run.started_at).toFixed(2)+'s after start':'did not run';
function select(id){const n=run.nodes.find(n=>n.id===id);if(!n)return;
document.querySelectorAll('[data-node]').forEach(el=>el.classList.toggle('selected',el.dataset.node===id));
const color=colors[n.status]||colors.missing;
document.getElementById('detail').innerHTML='<div class="detail-title"><div><h3>'+esc(n.name)+'</h3><code>'+esc(n.id)+' / '+esc(n.stage)+'</code></div><span class="badge" style="color:'+color+'">'+esc(n.status)+'</span></div><div class="meta-grid"><div><small>Started</small><span>'+relative(n.started_at)+'</span></div><div><small>Finished</small><span>'+relative(n.finished_at)+'</span></div><div><small>Dependencies</small><span>'+esc(n.needs.join(', ')||'entry point')+'</span></div><div><small>Result source</small><span>'+esc(n.status_source||'Recorded execution')+'</span></div></div><div class="metric"><small>SIMULATED METRIC</small>'+esc(n.metric)+'</div><ul class="tasks">'+n.tasks.map(t=>'<li><i class="dot" style="background:'+(colors[t.status]||colors.pending)+'"></i>'+esc(t.name)+'<span>'+esc(t.status)+'</span></li>').join('')+'</ul><pre class="logs">'+esc(n.events.length?n.events.map(e=>new Date(e.at*1000).toISOString().slice(11,23)+'  ['+e.part+'] '+e.message).join('\n'):(n.skip_reason||'No node telemetry was recorded.'))+'</pre>';
}
document.querySelectorAll('[data-node]').forEach(el=>{el.addEventListener('click',()=>select(el.dataset.node));if(el.tagName.toLowerCase()==='g')el.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();select(el.dataset.node)}})});
if(run.problems.length)document.getElementById('problems').innerHTML='<section class="panel"><div class="panel-head"><h2>Attention required</h2></div><ul class="error-list">'+run.problems.map(p=>'<li>'+esc(p)+'</li>').join('')+'</ul></section>';
select((run.nodes.find(n=>n.status==='failure')||run.nodes[0]).id);
</script></body></html>'''
    replacements = {"STATUS": payload["status"].upper(), "CARDS": cards, "ELAPSED": f"{payload['elapsed_seconds']:.1f}",
                    "TARGET": f"{payload['target_seconds']:g}", "GRAPH": graph_svg(nodes),
                    "HALF": f"{span / 2:.1f}", "SPAN": f"{span:.1f}", "TIMELINE": "".join(timeline),
                    "STARTED": escape(utc(payload["started_at"])), "DATA": embedded}
    for key, value in replacements.items():
        template = template.replace(f"@@{key}@@", value)
    return template


def report(data: dict, ctx: Context, actual: dict | None = None) -> int:
    records = resolve_records(data, ctx, actual or {})
    problems = []
    by_id = {node["id"]: node for node in data["nodes"]}
    for record in records:
        node = by_id[record["id"]]
        if record["status"] in {"failure", "cancelled", "missing"}:
            problems.append(f"{record['name']}: {record['status']}")
        elif record.get("telemetry_missing") and record["status"] == "success" and not node.get("optional", False):
            problems.append(f"{record['name']}: successful job is missing its telemetry artifact")
    final_node = by_id.get("report", {"id": "report", "name": "Publish observatory", "stage": "Report", "offset": ctx.baseline,
                                           "needs": list(by_id), "tasks": ["Collect telemetry", "Render DAG and timeline", "Publish reports"],
                                           "metric": "Interactive execution report generated"})
    record = blank_record(final_node, ctx)
    record["started_at"] = time.time()
    record["status"] = "running"
    event(record, "report", f"Collected {len(records)} nodes; checking recorded results against dependency state")
    for task in record["tasks"]:
        event(record, "report", f"{task['name']}")
        task["status"] = "success"
        task["started_at"] = time.time()
        task["finished_at"] = time.time()
    reserve = ctx.duration * 5 / ctx.baseline if ctx.local else 5.0
    until = ctx.started_at + max(0.0, ctx.duration - reserve)
    progress(record, ctx, "report", max(0.0, until - time.time()), "assemble final observatory")
    record["status"] = "failure" if problems else "success"
    record["finished_at"] = time.time()
    event(record, "report", f"Report result: {record['status']}; all quality metrics are simulated", "error" if problems else "success")
    save(path_for(ctx, "report"), record)
    records.append(record)
    run_id = os.environ.get("GITHUB_RUN_ID", "<run_id>")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "<attempt>")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    payload = {"kind": "demo-pipeline-report", "schema_version": 1, "simulated": True,
               "status": record["status"], "started_at": ctx.started_at, "finished_at": record["finished_at"],
               "target_seconds": ctx.duration, "elapsed_seconds": max(0.0, record["finished_at"] - ctx.started_at),
               "artifact_name": f"mission-control-{run_id}-{attempt}",
               "run_url": f"{server}/{repository}/actions/runs/{run_id}" if repository and run_id != "<run_id>" else None,
               "nodes": records, "problems": problems}
    save(ctx.reports / "report.json", payload)
    markdown = markdown_report(payload)
    (ctx.reports / "report.md").write_text(markdown, encoding="utf-8")
    (ctx.reports / "report.html").write_text(html_report(payload), encoding="utf-8")
    append(ctx.summary, markdown + "\n")
    emit(f"Reports: {ctx.reports / 'report.html'} · {len(records)} nodes · {payload['elapsed_seconds']:.2f}s", "success" if not problems else "error")
    for problem in problems:
        with LOCK:
            print(f"::error title=Demo pipeline result::{problem}", flush=True)
    return 1 if problems else 0


def local(data: dict, duration: float, fail_at: str, reports: Path) -> int:
    nodes = {node["id"]: node for node in data["nodes"] if node["id"] != "report"}
    if duration <= 0:
        raise ValueError("--duration must be positive")
    if fail_at not in {"none", *nodes}:
        raise ValueError(f"Unknown failure node: {fail_at}")
    ctx = Context(time.time(), duration, fail_at, reports.resolve(), float(data.get("target_seconds", 300)), local=True)
    pending = dict(nodes)
    results: dict[str, str] = {}
    running = {}

    def execute(node: dict) -> str:
        # A local run must never reuse another run's files or failure status.
        save(path_for(ctx, node["id"]), blank_record(node, ctx))
        try:
            stage(node, "prepare", ctx)
            stage(node, "work", ctx)
        except Exception as exc:
            emit(f"Local node {node['id']} failed: {exc}", "error")
        finally:
            stage(node, "finish", ctx)
        return load_record(node, ctx)["status"]

    emit(f"Starting local simulated DAG: {len(nodes) + 1} nodes · target {duration:g}s · failure={fail_at}")
    with ThreadPoolExecutor(max_workers=max(1, len(nodes))) as executor:
        while pending or running:
            for node_id, node in list(pending.items()):
                if any(parent not in results for parent in node.get("needs", [])):
                    continue
                blocked = any(results[parent] != "success" for parent in node.get("needs", []))
                skip = (node_id == "rollback" and results.get("canary") != "failure") or (blocked and node_id not in {"rollback", "cleanup"})
                if skip:
                    record = blank_record(node, ctx)
                    record["status"] = "skipped"
                    record["skip_reason"] = "Canary did not fail; rollback was not needed" if node_id == "rollback" else "A dependency did not succeed"
                    save(path_for(ctx, node_id), record)
                    results[node_id] = "skipped"
                    emit(f"[{node_id}] SKIPPED · {record['skip_reason']}", "warning")
                else:
                    running[executor.submit(execute, node)] = node_id
                del pending[node_id]
            if running:
                completed, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in completed:
                    node_id = running.pop(future)
                    try:
                        results[node_id] = future.result()
                    except Exception as exc:
                        results[node_id] = "failure"
                        emit(f"[{node_id}] Failed to finalize telemetry: {exc}", "error")
            elif pending:
                raise ValueError(f"No runnable nodes remain: {', '.join(pending)}")
    return report(data, ctx, {node_id: {"result": status} for node_id, status in results.items()})


def main() -> int:
    # Windows pipes may default to a legacy codepage; demo labels contain emoji.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    stage_parser = sub.add_parser("stage", help="Run one observable part of a simulated node")
    stage_parser.add_argument("--node", default=os.environ.get("DEMO_NODE"), required=not bool(os.environ.get("DEMO_NODE")))
    stage_parser.add_argument("--part", choices=("prepare", "work", "finish"), required=True)
    sub.add_parser("report", help="Collect node artifacts and render the final observatory")
    local_parser = sub.add_parser("local", help="Execute the same dependency DAG locally")
    local_parser.add_argument("--duration", type=float, default=3.0)
    local_parser.add_argument("--fail-at", default="none")
    local_parser.add_argument("--reports-dir", type=Path, default=Path(os.environ.get("DEMO_REPORTS_DIR", "reports")))
    args = parser.parse_args()
    data = config()
    if args.command == "local":
        return local(data, args.duration, args.fail_at, args.reports_dir)
    ctx = environment_context(data)
    if args.command == "report":
        raw = os.environ.get("DEMO_NEEDS_JSON", "{}")
        actual = json.loads(raw) if raw.strip() else {}
        if not isinstance(actual, dict):
            raise ValueError("DEMO_NEEDS_JSON must be a JSON object")
        return report(data, ctx, actual)
    node = next((node for node in data["nodes"] if node["id"] == args.node), None)
    if node is None:
        raise ValueError(f"Unknown node: {args.node}")
    if ctx.failure not in {"none", *[node["id"] for node in data["nodes"]]}:
        raise ValueError(f"Unknown DEMO_FAILURE node: {ctx.failure}")
    return stage(node, args.part, ctx)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError) as error:
        print(f"::error title=Demo simulator error::{error}", file=sys.stderr)
        raise SystemExit(2)
