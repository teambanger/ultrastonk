#!/usr/bin/env python3
"""Read-only terminal dashboard for Ultrastonk Docker Compose experiment stacks.

The dashboard groups containers by the Compose project label, parses the
event-emitter's replay log, reads Kafka topic offsets / consumer-group lag,
queries the Flink JobManager REST API, and watches evidence files.  It never
mutates a container, a Compose stack, or a file.

Usage examples
--------------
    python3 scripts/stack_tui.py --once
    python3 scripts/stack_tui.py --once --project ultrastonk-fullweek-2
    python3 scripts/stack_tui.py --once --no-stats --watch /tmp
    python3 scripts/stack_tui.py --interval 3 --project ultrastonk-fullweek-2
    python3 scripts/stack_tui.py --help

Interactive keys: q quit, r refresh, p pause/resume auto-refresh, ? help.

Standard library only.  Every Docker/exec call is bounded by a short timeout.
"""

import argparse
import curses
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

DOCKER_TIMEOUT = 4.0
STATS_TIMEOUT = 8.0
KAFKA_TIMEOUT = 5.0
HTTP_TIMEOUT = 3.0
LOG_TAIL = 400
ARTIFACT_NAMES = ("expected.jsonl", "output.jsonl", "audit.jsonl", "offsets.json", "emitter.log")
COUNT_CAP_BYTES = 64 * 1024 * 1024
DEFAULT_WATCH = "/mnt/shared/data/ultrastonk-full-week"
SERVICE_ORDER = {
    "kafka": 10,
    "kafka-init": 20,
    "event-emitter": 30,
    "jobmanager": 40,
    "taskmanager": 50,
    "gateway": 60,
}
KAFKA_TOOL_CLASSES = {
    "kafka-topics.sh": "kafka.tools.TopicCommand",
    "kafka-get-offsets.sh": "kafka.tools.GetOffsetShell",
    "kafka-consumer-groups.sh": "kafka.tools.ConsumerGroupCommand",
}


# ---------------------------------------------------------------------------
# small pure helpers
# ---------------------------------------------------------------------------

def short_reason(value, limit=220):
    text = "" if value is None else str(value)
    text = " ".join(text.split())
    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return text or "unknown error"


def parse_number(value):
    """Parse an integer/float that may contain thousands separators."""
    if value is None:
        return None
    text = str(value).strip().replace("_", "")
    if not text:
        return None
    if not re.fullmatch(r"[+-]?[\d,]+(?:\.\d+)?", text):
        return None
    text = text.replace(",", "")
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return None


def safe_percent(numerator, denominator):
    """Return a percentage or None for a zero/absent denominator."""
    num = parse_number(numerator)
    den = parse_number(denominator)
    if num is None or den is None or den <= 0:
        return None
    return (float(num) / float(den)) * 100.0


def compute_rate(events, elapsed):
    """Events per second, or None for zero/negative elapsed."""
    ev = parse_number(events)
    sec = parse_number(elapsed)
    if ev is None or sec is None or sec <= 0:
        return None
    return float(ev) / float(sec)


def eta_seconds(remaining, rate):
    """Seconds remaining, or None when the rate is zero/unknown."""
    rem = parse_number(remaining)
    speed = parse_number(rate)
    if rem is None or speed is None or speed <= 0:
        return None
    if rem <= 0:
        return 0.0
    return float(rem) / float(speed)


def format_int(value):
    num = parse_number(value)
    if num is None:
        return "unknown"
    if isinstance(num, float) and not num.is_integer():
        return f"{num:,.2f}"
    return f"{int(num):,}"


def format_eta(seconds):
    if seconds is None:
        return "unknown"
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return "unknown"
    if seconds < 0:
        return "unknown"
    if seconds == 0:
        return "0s"
    if seconds < 1:
        return "<1s"
    total = int(round(seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def format_bytes(value):
    num = parse_number(value)
    if num is None:
        return "unknown"
    size = float(num)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    for unit in units:
        if abs(size) < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} B"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PiB"


def format_epoch(epoch):
    num = parse_number(epoch)
    if num is None:
        return "unknown"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(num)))
    except (ValueError, OSError, OverflowError):
        return "unknown"


def truncate_line(line, width):
    text = "" if line is None else str(line)
    if width is None or width <= 0:
        return text
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


def _parse_labels(label_text):
    """Parse Docker's comma-separated label string without splitting values."""
    text = "" if label_text is None else str(label_text)
    labels = {}
    starts = list(re.finditer(r"(?:^|,)([A-Za-z0-9_.\-/]+)=", text))
    for index, match in enumerate(starts):
        key = match.group(1)
        value_start = match.end()
        value_end = starts[index + 1].start() if index + 1 < len(starts) else len(text)
        value = text[value_start:value_end]
        if value.endswith(","):
            value = value[:-1]
        labels[key] = value
    return labels


# ---------------------------------------------------------------------------
# emitter log parser
# ---------------------------------------------------------------------------

_BANNER_RE = re.compile(
    r"^Replaying\s+\[(?P<files>.*?)\]\s*->\s*topic\s+'(?P<topic>[^']+)'\s+"
    r"via\s+(?P<bootstrap>\S+)\s+at\s+(?P<speed>[\d.]+)x\s*\((?P<rest>.*)\)\s*$"
)
_PROGRESS_RE = re.compile(
    r"^\.\.\.\s+(?P<sent>[\d,]+)\s+events sent\s+"
    r"\((?P<rate>[\d,.]+)\s+ev/s,\s+(?P<elapsed>[\d,.]+)\s+s elapsed\)\s*$"
)
_FILE_START_RE = re.compile(r"^Replaying\s+(?P<path>.+?)\s*$")
_SUMMARY_RE = re.compile(
    r"^\s{2,}(?P<name>[^:]+):\s+read\s+(?P<read>[\d,]+)\s+->\s+"
    r"emitted\s+(?P<emitted>[\d,]+)\s+\((?P<drop>[\d.]+)% dropped"
)
_TOTAL_RE = re.compile(
    r"^Total:\s+read\s+(?P<read>[\d,]+)\s+->\s+emitted\s+(?P<emitted>[\d,]+)\s+"
    r"\((?P<drop>[\d.]+)% dropped"
)
_DONE_RE = re.compile(
    r"^Done:\s+(?P<submitted>[\d,]+)\s+submitted,\s+(?P<failed>[\d,]+)\s+failed,\s+"
    r"(?P<rate>[\d,.]+)\s+ev/s,\s+first (?:paced|event) time\s+(?P<first>[\d,]+)"
)


def _basename(path):
    return str(path).replace("\\", "/").rstrip("/").split("/")[-1]


def _parse_banner_rest(rest):
    result = {"event_zone": None, "price_filter": None, "pacing": None}
    text = "" if rest is None else str(rest)
    zone = re.search(r"event zone\s+([^,]+)", text)
    price = re.search(r"price filter\s+([^,]+)", text)
    pacing = re.search(r"pacing\s+([^)]+)", text)
    if zone:
        result["event_zone"] = zone.group(1).strip()
    if price:
        result["price_filter"] = price.group(1).strip()
    if pacing:
        result["pacing"] = pacing.group(1).strip()
    return result


def parse_emitter_log(text):
    """Parse the emitter's stdout.  Garbage and truncated lines are ignored."""
    progress = {
        "banner": None,
        "files": {},
        "file_order": [],
        "current_file": None,
        "samples": [],
        "events_sent": None,
        "rate_ev_s": None,
        "elapsed_s": None,
        "done": False,
        "done_stats": None,
        "total": None,
        "expected_total": None,
        "expected_total_source": None,
        "expected_total_complete": False,
        "warnings": [],
    }
    for raw_line in (text or "").splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        banner = _BANNER_RE.match(line)
        if banner:
            files = [part.strip() for part in banner.group("files").split(",") if part.strip()]
            rest = _parse_banner_rest(banner.group("rest"))
            progress["banner"] = {
                "files": files,
                "topic": banner.group("topic"),
                "bootstrap": banner.group("bootstrap"),
                "speed_multiplier": parse_number(banner.group("speed")),
                "event_zone": rest["event_zone"],
                "price_filter": rest["price_filter"],
                "pacing": rest["pacing"],
            }
            continue
        match = _PROGRESS_RE.match(line)
        if match:
            sent = parse_number(match.group("sent"))
            rate = parse_number(match.group("rate"))
            elapsed = parse_number(match.group("elapsed"))
            progress["events_sent"] = sent
            progress["rate_ev_s"] = rate
            progress["elapsed_s"] = elapsed
            progress["samples"].append({"events": sent, "rate": rate, "elapsed": elapsed})
            continue
        match = _SUMMARY_RE.match(line)
        if match:
            name = match.group("name").strip()
            entry = progress["files"].setdefault(name, {"name": name, "started": False, "completed": False})
            entry["read"] = parse_number(match.group("read"))
            entry["emitted"] = parse_number(match.group("emitted"))
            entry["drop_percent"] = parse_number(match.group("drop"))
            entry["completed"] = True
            if name not in progress["file_order"]:
                progress["file_order"].append(name)
            continue
        match = _TOTAL_RE.match(line)
        if match:
            progress["total"] = {
                "read": parse_number(match.group("read")),
                "emitted": parse_number(match.group("emitted")),
                "drop_percent": parse_number(match.group("drop")),
            }
            continue
        match = _DONE_RE.match(line)
        if match:
            progress["done"] = True
            progress["done_stats"] = {
                "submitted": parse_number(match.group("submitted")),
                "failed": parse_number(match.group("failed")),
                "rate_ev_s": parse_number(match.group("rate")),
                "first_paced_time": parse_number(match.group("first")),
            }
            continue
        match = _FILE_START_RE.match(line)
        if match and "-> topic" not in line:
            name = _basename(match.group("path"))
            entry = progress["files"].setdefault(name, {"name": name, "started": False, "completed": False})
            entry["started"] = True
            entry["path"] = match.group("path").strip()
            if name not in progress["file_order"]:
                progress["file_order"].append(name)
            progress["current_file"] = name
            continue
        # Anything else (SLF4J warnings, partial lines, etc.) is ignored.

    if progress["done"] and progress["done_stats"]:
        if progress["events_sent"] is None:
            progress["events_sent"] = progress["done_stats"].get("submitted")
        if progress["rate_ev_s"] is None:
            progress["rate_ev_s"] = progress["done_stats"].get("rate_ev_s")

    total = progress["total"]
    if total and total.get("read") is not None:
        progress["expected_total"] = total["read"]
        progress["expected_total_source"] = "total"
        progress["expected_total_complete"] = True
    else:
        reads = [entry.get("read") for entry in progress["files"].values() if entry.get("read") is not None]
        if reads:
            progress["expected_total"] = sum(int(value) for value in reads)
            progress["expected_total_source"] = "sum-file-reads"
        banner = progress["banner"]
        if banner and banner.get("files"):
            expected_names = {_basename(path) for path in banner["files"]}
            seen = {name for name, entry in progress["files"].items() if entry.get("completed")}
            progress["expected_total_complete"] = bool(expected_names) and expected_names.issubset(seen)
        else:
            progress["expected_total_complete"] = False
    return progress


def derive_emitter_view(progress):
    """Turn parser output into display metrics (pure)."""
    view = {
        "topic": None,
        "bootstrap": None,
        "speed_multiplier": None,
        "current_file": None,
        "current_file_completed": False,
        "current_file_percent": None,
        "run_percent": None,
        "rate_ev_s": None,
        "rate_source": "unknown",
        "eta_current_s": None,
        "eta_total_s": None,
        "expected_total": None,
        "expected_total_source": None,
        "expected_total_complete": False,
        "finished": False,
        "done_stats": None,
        "total": None,
        "files": [],
        "warnings": [],
    }
    if not isinstance(progress, dict):
        return view
    banner = progress.get("banner") or {}
    view["topic"] = banner.get("topic")
    view["bootstrap"] = banner.get("bootstrap")
    view["speed_multiplier"] = banner.get("speed_multiplier")
    current = progress.get("current_file")
    view["current_file"] = current
    files = progress.get("files") or {}
    current_entry = files.get(current) if current else None
    if current_entry:
        view["current_file_completed"] = bool(current_entry.get("completed"))
        if current_entry.get("completed") and current_entry.get("read") is not None:
            view["current_file_percent"] = safe_percent(current_entry.get("emitted"), current_entry.get("read"))

    events_sent = progress.get("events_sent")
    completed_emitted = sum(
        int(entry.get("emitted") or 0)
        for entry in files.values()
        if entry.get("completed") and entry.get("emitted") is not None
    )
    active_emitted = None
    if events_sent is not None:
        active_emitted = max(0, int(events_sent) - completed_emitted)
    if current_entry and not current_entry.get("completed") and current_entry.get("read") is not None:
        view["current_file_percent"] = safe_percent(active_emitted, current_entry.get("read"))

    rate = progress.get("rate_ev_s")
    rate_source = "logged average" if rate is not None else "unknown"
    samples = progress.get("samples") or []
    if len(samples) >= 2:
        first, last = samples[-2], samples[-1]
        try:
            events_delta = int(last.get("events")) - int(first.get("events"))
            elapsed_delta = float(last.get("elapsed")) - float(first.get("elapsed"))
        except (TypeError, ValueError):
            events_delta = 0
            elapsed_delta = 0.0
        if events_delta > 0 and elapsed_delta > 0:
            rate = float(events_delta) / elapsed_delta
            rate_source = "instantaneous (last 2 log samples)"
    view["rate_ev_s"] = rate
    view["rate_source"] = rate_source

    expected_total = progress.get("expected_total")
    complete = bool(progress.get("expected_total_complete"))
    view["expected_total"] = expected_total
    view["expected_total_source"] = progress.get("expected_total_source")
    view["expected_total_complete"] = complete
    finished = bool(progress.get("done"))
    total_entry = progress.get("total") or {}
    if finished and total_entry.get("read") is not None and total_entry.get("emitted") is not None:
        view["run_percent"] = safe_percent(total_entry.get("emitted"), total_entry.get("read"))
    elif expected_total is not None and complete:
        view["run_percent"] = safe_percent(events_sent, expected_total)
    if expected_total is not None and complete:
        view["eta_total_s"] = 0.0 if finished else eta_seconds(int(expected_total) - int(events_sent or 0), rate)

    if current_entry:
        if current_entry.get("completed"):
            view["eta_current_s"] = 0.0
        elif current_entry.get("read") is not None and active_emitted is not None:
            view["eta_current_s"] = eta_seconds(int(current_entry["read"]) - active_emitted, rate)

    view["finished"] = finished
    view["done_stats"] = progress.get("done_stats")
    view["total"] = progress.get("total")
    view["warnings"] = progress.get("warnings") or []
    for name in progress.get("file_order") or []:
        entry = files.get(name) or {}
        view["files"].append({
            "name": name,
            "read": entry.get("read"),
            "emitted": entry.get("emitted"),
            "completed": bool(entry.get("completed")),
            "drop_percent": entry.get("drop_percent"),
        })
    return view


# ---------------------------------------------------------------------------
# Kafka / Docker / df / HTTP parsers
# ---------------------------------------------------------------------------

_OFFSET_RE = re.compile(r"^(?P<topic>.+?):(?P<partition>-?\d+):(?P<offset>-?\d+)$")


def parse_kafka_offsets(text):
    """Parse kafka-get-offsets.sh output: topic:partition:offset."""
    result = {}
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _OFFSET_RE.match(line)
        if not match:
            continue
        topic = match.group("topic")
        partition = int(match.group("partition"))
        offset = int(match.group("offset"))
        result.setdefault(topic, {})[partition] = offset
    return result


def offset_deltas(previous, current):
    """Return current - previous for each topic/partition; None when absent."""
    if not previous:
        return None
    result = {}
    for topic, partitions in (current or {}).items():
        for partition, offset in partitions.items():
            old = (previous.get(topic) or {}).get(partition)
            result.setdefault(topic, {})[partition] = None if old is None else offset - old
    return result


def total_offsets(offsets):
    return {topic: sum(partitions.values()) for topic, partitions in (offsets or {}).items()}


def parse_kafka_topics(text, project):
    """Return project-prefixed topic names, in/out/audit first."""
    topics = []
    for raw_line in (text or "").splitlines():
        name = raw_line.strip()
        if not name or name.startswith("__"):
            continue
        if not (name == project or name.startswith(project + "-") or name.startswith(project + "_")):
            continue
        if name not in topics:
            topics.append(name)
    order = {"-in": 0, "-out": 1, "-audit": 2}

    def sort_key(name):
        for suffix, rank in order.items():
            if name.endswith(suffix):
                return (rank, name)
        return (9, name)

    return sorted(topics, key=sort_key)


def parse_consumer_groups_list(text):
    groups = []
    for raw_line in (text or "").splitlines():
        name = raw_line.strip()
        if not name or name.startswith("GROUP") or name.startswith("Error"):
            continue
        if name not in groups:
            groups.append(name)
    return groups


def matching_groups(groups, project):
    return [name for name in groups if name == project or name.startswith(project + "-") or name.startswith(project + "_")]


def parse_consumer_group(text):
    """Parse kafka-consumer-groups.sh --describe output."""
    result = {
        "available": True,
        "exists": None,
        "active": None,
        "partitions": [],
        "total_lag": None,
        "lag_complete": False,
        "note": None,
    }
    raw = text or ""
    lower = raw.lower()
    if "does not exist" in lower or "group not found" in lower:
        result["exists"] = False
        result["note"] = "group not found"
        return result
    if "has no active members" in lower:
        result["active"] = False
    lines = [line for line in raw.splitlines() if line.strip()]
    if not lines:
        result["available"] = False
        result["note"] = "empty output"
        return result
    header = None
    for line in lines:
        if "GROUP" in line and "TOPIC" in line and "PARTITION" in line:
            header = line.split()
            break
    if header is None:
        result["available"] = False
        result["note"] = "unrecognized output"
        return result
    columns = {name: index for index, name in enumerate(header)}
    for line in lines:
        if line is header or "GROUP" in line and "TOPIC" in line:
            continue
        fields = line.split()
        if len(fields) < 4:
            continue
        try:
            topic = fields[columns["TOPIC"]]
            partition = int(fields[columns["PARTITION"]])
        except (KeyError, ValueError, IndexError):
            continue
        current = parse_number(fields[columns.get("CURRENT-OFFSET", -1)]) if "CURRENT-OFFSET" in columns else None
        log_end = parse_number(fields[columns.get("LOG-END-OFFSET", -1)]) if "LOG-END-OFFSET" in columns else None
        lag = parse_number(fields[columns.get("LAG", -1)]) if "LAG" in columns else None
        result["partitions"].append({
            "topic": topic,
            "partition": partition,
            "current": current,
            "log_end": log_end,
            "lag": lag,
        })
    if result["partitions"]:
        result["exists"] = True
        if result["active"] is None:
            result["active"] = True
        lags = [p["lag"] for p in result["partitions"] if p["lag"] is not None]
        result["total_lag"] = sum(lags) if lags else None
        result["lag_complete"] = len(lags) == len(result["partitions"])
    return result


_PORT_RE = re.compile(
    r"^(?P<container_port>\d+)/(?P<protocol>tcp|udp)\s*->\s*(?P<host>\[[^\]]+\]|[^:]+):(?P<host_port>\d+)$"
)


def parse_docker_port(text):
    mappings = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _PORT_RE.match(line)
        if not match:
            continue
        mappings.append({
            "container_port": int(match.group("container_port")),
            "protocol": match.group("protocol"),
            "host_ip": match.group("host"),
            "host_port": int(match.group("host_port")),
        })
    return mappings


def _host_ip_for_url(mapping):
    host = (mapping or {}).get("host_ip") or "127.0.0.1"
    if host in ("0.0.0.0", "::", "[::]"):
        return "127.0.0.1"
    if host.startswith("[") and host.endswith("]"):
        return host[1:-1]
    return host


def parse_docker_stats(text):
    """Parse one-or-more JSON lines from docker stats --format json."""
    result = {}
    raw = (text or "").strip()
    if not raw:
        return result
    records = []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            records = parsed
        elif isinstance(parsed, dict):
            records = [parsed]
    except json.JSONDecodeError:
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                records.append(item)
    for item in records:
        name = item.get("Name") or item.get("Container")
        if not name:
            continue
        entry = {
            "name": name,
            "cpu_percent": parse_number(str(item.get("CPUPerc", "")).rstrip("%")),
            "mem_percent": parse_number(str(item.get("MemPerc", "")).rstrip("%")),
            "mem_usage": item.get("MemUsage"),
            "net_io": item.get("NetIO"),
            "block_io": item.get("BlockIO"),
            "pids": item.get("PIDs"),
        }
        result[name] = entry
        short_id = item.get("ID")
        if short_id:
            result[short_id] = entry
    return result


def parse_docker_ps(text):
    """Parse docker ps --format '{{json .}}' output."""
    containers = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        labels = _parse_labels(item.get("Labels"))
        name = (item.get("Names") or "").split(",")[0].strip()
        container_id = item.get("ID") or ""
        containers.append({
            "id": container_id,
            "short_id": container_id[:12],
            "name": name,
            "image": item.get("Image") or "",
            "state": item.get("State") or "",
            "status": item.get("Status") or "",
            "health": item.get("HealthStatus") or "none",
            "running_for": item.get("RunningFor") or "",
            "project": labels.get("com.docker.compose.project"),
            "service": labels.get("com.docker.compose.service"),
            "oneoff": labels.get("com.docker.compose.oneoff", "").lower() == "true",
        })
    return containers


_DF_RE = re.compile(
    r"^(?P<source>\S+)\s+(?P<size>\S+)\s+(?P<used>\S+)\s+(?P<avail>\S+)\s+(?P<pcent>\d+)%\s+(?P<target>.+)$"
)


def parse_df_rows(text):
    rows = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _DF_RE.match(line)
        if not match:
            continue
        rows.append({
            "source": match.group("source"),
            "size": match.group("size"),
            "used": match.group("used"),
            "avail": match.group("avail"),
            "percent": int(match.group("pcent")),
            "target": match.group("target").strip(),
        })
    return rows


def parse_flink_overview(text):
    try:
        data = json.loads(text or "")
    except json.JSONDecodeError:
        return {"available": False, "error": "invalid JSON", "jobs": []}
    if not isinstance(data, dict) or not isinstance(data.get("jobs"), list):
        return {"available": False, "error": "unexpected JSON shape", "jobs": []}
    return {"available": True, "error": None, "jobs": data["jobs"]}


def parse_flink_job(text):
    try:
        data = json.loads(text or "")
    except json.JSONDecodeError:
        return {"available": False, "error": "invalid JSON", "job": None}
    if not isinstance(data, dict):
        return {"available": False, "error": "unexpected JSON shape", "job": None}
    return {"available": True, "error": None, "job": data}


def _vertex_metric(vertex, *names):
    metrics = vertex.get("metrics") if isinstance(vertex, dict) else None
    if not isinstance(metrics, dict):
        return None
    for name in names:
        if name in metrics:
            return metrics[name]
    return None


def summarize_flink_job(overview, detail, vertices=None):
    """Combine overview/detail/vertices JSON into a display-friendly summary."""
    summary = {
        "available": False,
        "error": None,
        "name": None,
        "state": None,
        "jid": None,
        "start_time": None,
        "duration": None,
        "vertices": [],
        "jobs_count": 0,
    }
    if not overview or not overview.get("available"):
        summary["error"] = (overview or {}).get("error") or "overview unavailable"
        return summary
    jobs = overview.get("jobs") or []
    summary["jobs_count"] = len(jobs)
    job = None
    if detail and detail.get("available") and detail.get("job"):
        job = detail["job"]
    elif jobs:
        job = jobs[0]
    if not isinstance(job, dict):
        summary["available"] = True
        return summary
    summary["available"] = True
    summary["name"] = job.get("name")
    summary["state"] = job.get("state")
    summary["jid"] = job.get("jid")
    summary["start_time"] = job.get("start-time")
    summary["duration"] = job.get("duration")
    vertex_source = []
    if isinstance(vertices, dict) and isinstance(vertices.get("vertices"), list):
        vertex_source = vertices["vertices"]
    elif isinstance(job.get("vertices"), list):
        vertex_source = job["vertices"]
    for vertex in vertex_source:
        if not isinstance(vertex, dict):
            continue
        summary["vertices"].append({
            "name": vertex.get("name"),
            "status": vertex.get("status"),
            "parallelism": vertex.get("parallelism"),
            "num_records_in": _vertex_metric(vertex, "numRecordsIn", "read-records"),
            "num_records_out": _vertex_metric(vertex, "numRecordsOut", "write-records"),
            "backpressure": _vertex_metric(vertex, "accumulated-backpressured-time", "backpressure", "backpressure-level"),
            "backpressure_idle": _vertex_metric(vertex, "accumulated-idle-time"),
        })
    return summary


def parse_flink_log_state(text):
    """Best-effort last job state from JobManager JSON log lines."""
    observed = None
    job_name = None
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            record = None
        if isinstance(record, dict):
            message = str(record.get("message") or "")
            timestamp = record.get("timestamp")
        else:
            message = line
            timestamp = None
        match = re.search(r"job '([^']+)'", message)
        if match:
            job_name = match.group(1)
            if observed:
                observed["job_name"] = job_name
        lowered = message.lower()
        state = None
        if "application completed successfully" in lowered:
            state = "SUCCEEDED"
        elif "application completed with status" in lowered:
            match = re.search(r"status\s+([A-Za-z]+)", message, re.IGNORECASE)
            state = match.group(1).upper() if match else "COMPLETED"
        elif "application status succeeded" in lowered:
            state = "SUCCEEDED"
        elif "application status failed" in lowered:
            state = "FAILED"
        elif "terminating cluster entrypoint" in lowered and "exit code" in lowered:
            match = re.search(r"exit code\s+(\d+)", message)
            state = "SUCCEEDED" if match and match.group(1) == "0" else "FAILED"
        if state:
            observed = {
                "state": state,
                "timestamp": timestamp,
                "job_name": job_name,
                "message": message,
            }
    return observed


# ---------------------------------------------------------------------------
# bounded I/O helpers
# ---------------------------------------------------------------------------

def run_cmd(args, timeout=DOCKER_TIMEOUT, combine=False):
    """Run a command with a hard timeout.  Never raises."""
    try:
        if combine:
            proc = subprocess.run(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                timeout=timeout,
            )
            return {"rc": proc.returncode, "out": proc.stdout or "", "err": "", "timed_out": False}
        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            timeout=timeout,
        )
        return {"rc": proc.returncode, "out": proc.stdout or "", "err": proc.stderr or "", "timed_out": False}
    except subprocess.TimeoutExpired:
        return {"rc": -1, "out": "", "err": f"timed out after {timeout:g}s", "timed_out": True}
    except FileNotFoundError:
        return {"rc": 127, "out": "", "err": "command not found", "timed_out": False}
    except OSError as exc:
        return {"rc": 126, "out": "", "err": str(exc), "timed_out": False}


def _docker_unavailable_reason():
    if shutil.which("docker") is None:
        return "docker executable not found"
    result = run_cmd(["docker", "ps", "-a", "--format", "{{json .}}"], timeout=DOCKER_TIMEOUT)
    if result["timed_out"]:
        return "docker ps timed out"
    if result["rc"] != 0:
        return short_reason(result["err"] or result["out"])
    return None


def _looks_like_missing_tool(text):
    lowered = (text or "").lower()
    return any(
        marker in lowered
        for marker in (
            "no such file or directory",
            "executable file not found",
            "command not found",
            "could not find or load main class",
            "classnotfoundexception",
            "oci runtime exec failed",
        )
    )


def _kafka_failed(text):
    lowered = (text or "").lower()
    return "connection refused" in lowered or "error" in lowered or "timed out" in lowered


def _run_kafka_tool(container, tool, args, timeout=KAFKA_TIMEOUT):
    candidates = [
        ["/opt/kafka/bin/" + tool] + args,
        [tool] + args,
        ["/opt/kafka/bin/kafka-run-class.sh", KAFKA_TOOL_CLASSES[tool]] + args,
        ["kafka-run-class.sh", KAFKA_TOOL_CLASSES[tool]] + args,
    ]
    last = None
    for command in candidates:
        result = run_cmd(["docker", "exec", container] + command, timeout=timeout)
        last = result
        text = (result.get("out") or "") + (result.get("err") or "")
        if result.get("timed_out"):
            return result
        if _looks_like_missing_tool(text):
            continue
        return result
    return last or {"rc": 127, "out": "", "err": "no kafka tool candidate", "timed_out": False}


def _kafka_bootstrap_candidates(container, mappings):
    ports = []
    for mapping in mappings:
        port = mapping.get("container_port")
        if port and port not in ports:
            ports.append(port)
    ordered = [9092, 29092]
    ordered.extend(port for port in ports if port not in ordered)
    candidates = []
    for port in ordered:
        candidate = f"localhost:{port}"
        if candidate not in candidates:
            candidates.append(candidate)
    env_result = run_cmd(["docker", "inspect", container, "--format", "{{json .Config.Env}}"], timeout=DOCKER_TIMEOUT)
    if env_result["rc"] == 0:
        try:
            env_values = json.loads(env_result["out"] or "[]")
        except json.JSONDecodeError:
            env_values = []
        for value in env_values:
            if not isinstance(value, str):
                continue
            if value.startswith("KAFKA_ADVERTISED_LISTENERS") or value.startswith("KAFKA_LISTENERS"):
                for port in re.findall(r":(\d+)", value):
                    candidate = f"localhost:{port}"
                    if candidate not in candidates:
                        candidates.append(candidate)
    return candidates


def _detect_kafka(container, mappings):
    errors = []
    for candidate in _kafka_bootstrap_candidates(container, mappings):
        result = _run_kafka_tool(container, "kafka-topics.sh", ["--bootstrap-server", candidate, "--list"])
        text = (result.get("out") or "") + (result.get("err") or "")
        if result.get("rc") == 0 and not _kafka_failed(text):
            return candidate, result.get("out") or "", errors
        errors.append(f"{candidate}: {short_reason(result.get('err') or result.get('out'))}")
    return None, "", errors


def _fetch_json(url, timeout=HTTP_TIMEOUT):
    try:
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(4 * 1024 * 1024)
        return json.loads(raw.decode("utf-8", "replace")), None
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError) as exc:
        return None, short_reason(exc)


def _count_lines(path, cap=COUNT_CAP_BYTES):
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        return None, short_reason(exc)
    if size > cap:
        return None, None
    try:
        count = 0
        last_byte = b""
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                count += chunk.count(b"\n")
                last_byte = chunk[-1:]
        if size and last_byte and last_byte != b"\n":
            count += 1
        return count, None
    except OSError as exc:
        return None, short_reason(exc)


def _fs_usage(path):
    result = run_cmd(["df", "-h", "--output=source,size,used,avail,pcent,target", path], timeout=3.0)
    rows = parse_df_rows(result.get("out") or "")
    if not rows:
        result = run_cmd(["df", "-h", path], timeout=3.0)
        rows = parse_df_rows(result.get("out") or "")
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# collection
# ---------------------------------------------------------------------------

def _service_sort_key(container):
    service = container.get("service") or container.get("name") or ""
    base = service
    for suffix in ("-1", "-2", "-3", "-4", "-5", "-6", "-7", "-8", "-9"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    rank = SERVICE_ORDER.get(base, 100)
    return (rank, base, container.get("name") or "")


def _container_ports(container):
    result = run_cmd(["docker", "port", container.get("name") or container.get("id") or ""], timeout=2.0)
    if result.get("rc") != 0:
        return []
    return parse_docker_port(result.get("out") or "")


def _collect_emitter(containers):
    candidates = [
        item for item in containers
        if "event-emitter" in ((item.get("service") or "") + " " + (item.get("image") or "")).lower()
    ]
    if not candidates:
        return {"available": False, "error": "no event-emitter container", "containers": []}
    parsed = []
    for container in candidates:
        result = run_cmd(["docker", "logs", "--tail", str(LOG_TAIL), container.get("name") or ""], timeout=4.0, combine=True)
        text = result.get("out") or ""
        if result.get("rc") != 0 and not text.strip():
            parsed.append({"container": container.get("name"), "available": False, "error": short_reason(result.get("err") or "docker logs failed")})
            continue
        progress = parse_emitter_log(text)
        parsed.append({"container": container.get("name"), "available": True, "error": None, "progress": progress})
    first = parsed[0]
    first["containers"] = parsed
    return first


def _collect_kafka(containers, project, state, now):
    kafka_containers = [item for item in containers if (item.get("service") or "") == "kafka"]
    if not kafka_containers:
        return {"available": False, "error": "no kafka container"}
    container = kafka_containers[0]
    mappings = _container_ports(container)
    bootstrap, topics_text, errors = _detect_kafka(container.get("name") or "", mappings)
    if bootstrap is None:
        return {"available": False, "error": "kafka unreachable: " + short_reason("; ".join(errors[-2:]))}
    topics = parse_kafka_topics(topics_text, project)
    offsets = {}
    topic_errors = []
    for topic in topics:
        result = _run_kafka_tool(container.get("name") or "", "kafka-get-offsets.sh", ["--bootstrap-server", bootstrap, "--topic", topic])
        text = (result.get("out") or "") + (result.get("err") or "")
        if result.get("rc") != 0 or _kafka_failed(text):
            topic_errors.append(f"{topic}: {short_reason(result.get('err') or result.get('out'))}")
            continue
        parsed = parse_kafka_offsets(result.get("out") or "")
        for name, partitions in parsed.items():
            offsets.setdefault(name, {}).update(partitions)
    previous = state.get("kafka_prev", {}).get(project)
    deltas = offset_deltas(previous.get("offsets") if previous else None, offsets)
    state.setdefault("kafka_prev", {})[project] = {"offsets": offsets, "at": now, "bootstrap": bootstrap}
    groups = []
    group_list = _run_kafka_tool(container.get("name") or "", "kafka-consumer-groups.sh", ["--bootstrap-server", bootstrap, "--list"])
    if group_list.get("rc") == 0:
        for group in matching_groups(parse_consumer_groups_list(group_list.get("out") or ""), project):
            described = _run_kafka_tool(container.get("name") or "", "kafka-consumer-groups.sh", ["--bootstrap-server", bootstrap, "--describe", "--group", group])
            parsed = parse_consumer_group((described.get("out") or "") + (described.get("err") or ""))
            parsed["name"] = group
            groups.append(parsed)
    return {
        "available": True,
        "error": None,
        "bootstrap": bootstrap,
        "topics": topics,
        "offsets": offsets,
        "totals": total_offsets(offsets),
        "deltas": deltas,
        "groups": groups,
        "errors": topic_errors,
    }


def _valid_ip(value):
    text = (value or "").strip()
    if not text or " " in text:
        return None
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", text):
        return text
    if ":" in text and re.fullmatch(r"[0-9a-fA-F:]+", text):
        return text
    return None


def _flink_last_from_logs(manager):
    result = run_cmd(["docker", "logs", "--tail", "200", manager.get("name") or ""], timeout=3.0, combine=True)
    if result.get("rc") != 0 and not result.get("out"):
        return None
    observed = parse_flink_log_state(result.get("out") or "")
    if not observed or not observed.get("state"):
        return None
    summary = {
        "available": True,
        "error": None,
        "name": observed.get("job_name"),
        "state": observed.get("state"),
        "jid": None,
        "start_time": None,
        "duration": None,
        "vertices": [],
        "jobs_count": 1,
    }
    return {
        "available": True,
        "stale": True,
        "last_seen": observed.get("timestamp"),
        "last_seen_text": observed.get("timestamp"),
        "summary": summary,
        "error": "jobmanager REST unavailable; last state from container logs",
    }


def _collect_flink(containers, project, state, now):
    managers = [item for item in containers if (item.get("service") or "") == "jobmanager"]
    if not managers:
        return {"available": False, "error": "no jobmanager container"}
    manager = managers[0]
    mappings = _container_ports(manager)
    rest_mapping = next((mapping for mapping in mappings if mapping.get("container_port") == 8081), None)
    base_url = None
    if rest_mapping:
        base_url = f"http://{_host_ip_for_url(rest_mapping)}:{rest_mapping['host_port']}"
    else:
        inspect = run_cmd(["docker", "inspect", manager.get("name") or "", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"], timeout=DOCKER_TIMEOUT)
        ip = _valid_ip(inspect.get("out"))
        if ip:
            base_url = f"http://{ip}:8081"
    cache = state.setdefault("flink_cache", {}).get(project)
    if not base_url:
        if cache:
            return {"available": True, "stale": True, "last_seen": cache.get("at"), "summary": cache.get("summary"), "error": "jobmanager REST port unavailable"}
        fallback = _flink_last_from_logs(manager)
        if fallback:
            return fallback
        reason = "jobmanager container is exited" if manager.get("state") != "running" else "jobmanager REST port unavailable"
        return {"available": False, "error": reason}
    overview_data, overview_error = _fetch_json(base_url + "/jobs/overview")
    if overview_data is None:
        if cache:
            return {"available": True, "stale": True, "last_seen": cache.get("at"), "summary": cache.get("summary"), "error": "REST unavailable: " + short_reason(overview_error)}
        fallback = _flink_last_from_logs(manager)
        if fallback:
            return fallback
        return {"available": False, "error": "REST unavailable: " + short_reason(overview_error)}
    overview = parse_flink_overview(json.dumps(overview_data))
    jobs = overview.get("jobs") or []
    detail = None
    vertices = None
    if jobs:
        jid = jobs[0].get("jid") if isinstance(jobs[0], dict) else None
        if jid:
            detail_data, _ = _fetch_json(base_url + "/jobs/" + str(jid))
            if detail_data is not None:
                detail = parse_flink_job(json.dumps(detail_data))
            vertices_data, _ = _fetch_json(base_url + "/jobs/" + str(jid) + "/vertices")
            if isinstance(vertices_data, dict) and isinstance(vertices_data.get("vertices"), list):
                vertices = vertices_data
    summary = summarize_flink_job(overview, detail, vertices)
    if summary.get("available") and summary.get("name"):
        state.setdefault("flink_cache", {})[project] = {"summary": summary, "at": now}
        return {"available": True, "stale": False, "summary": summary, "error": None, "url": base_url}
    if cache:
        return {"available": True, "stale": True, "last_seen": cache.get("at"), "summary": cache.get("summary"), "error": "no current job; last observed state", "url": base_url}
    fallback = _flink_last_from_logs(manager)
    if fallback:
        return fallback
    return {"available": True, "stale": False, "summary": summary, "error": None, "url": base_url}


def _collect_artifacts(watch_dirs, state, now):
    report = {"dirs": [], "files": [], "errors": []}
    previous = state.get("artifacts", {})
    new_state = {}
    for directory in watch_dirs:
        directory = os.path.abspath(directory)
        if not os.path.isdir(directory):
            report["dirs"].append({"path": directory, "available": False, "error": "not found"})
            report["errors"].append(f"{directory}: not found")
            continue
        report["dirs"].append({"path": directory, "available": True, "error": None})
        for root, dirs, files in os.walk(directory):
            depth = os.path.relpath(root, directory).count(os.sep)
            if depth >= 4:
                dirs[:] = []
            for name in files:
                if name not in ARTIFACT_NAMES:
                    continue
                path = os.path.join(root, name)
                try:
                    stat = os.stat(path)
                except OSError as exc:
                    report["errors"].append(f"{path}: {short_reason(exc)}")
                    continue
                lines, line_error = _count_lines(path)
                old = previous.get(path)
                growth = None
                if old and old.get("size") is not None and now > old.get("at", now):
                    growth = (stat.st_size - old["size"]) / max(now - old["at"], 1e-9)
                report["files"].append({
                    "path": path,
                    "name": name,
                    "size": stat.st_size,
                    "lines": lines,
                    "line_error": line_error,
                    "growth_bps": growth,
                    "mtime": stat.st_mtime,
                })
                new_state[path] = {"size": stat.st_size, "at": now, "mtime": stat.st_mtime}
    state["artifacts"] = new_state
    report["files"].sort(key=lambda item: item["path"])
    return report


def _collect_host(watch_dirs):
    host = {"load": None, "filesystems": [], "tmpfs": None}
    try:
        host["load"] = os.getloadavg()
    except (OSError, AttributeError):
        pass
    seen = set()
    paths = ["/mnt/shared"] + list(watch_dirs)
    for path in paths:
        if not path or not os.path.exists(path):
            continue
        row = _fs_usage(path)
        if row and row["target"] not in seen:
            seen.add(row["target"])
            host["filesystems"].append(row)
    if os.path.isdir("/tmp"):
        row = _fs_usage("/tmp")
        if row and (row["source"].startswith("tmpfs") or row["target"] == "/tmp") and row["target"] not in seen:
            host["tmpfs"] = row
    return host


def collect_snapshot(args, state):
    """Collect one read-only snapshot.  Panels degrade instead of raising."""
    now = time.time()
    requested = list(getattr(args, "project", []) or [])
    snapshot = {
        "timestamp": now,
        "projects": [],
        "docker_error": None,
        "artifacts": None,
        "host": None,
        "requested": requested,
        "warnings": [],
    }
    try:
        docker_error = _docker_unavailable_reason()
        if docker_error:
            snapshot["docker_error"] = docker_error
            return snapshot
        ps = run_cmd(["docker", "ps", "-a", "--no-trunc", "--format", "{{json .}}"], timeout=DOCKER_TIMEOUT)
        if ps["timed_out"] or ps["rc"] != 0:
            snapshot["docker_error"] = short_reason(ps.get("err") or ps.get("out") or "docker ps failed")
            return snapshot
        containers = parse_docker_ps(ps.get("out") or "")
        stats = {}
        if not getattr(args, "no_stats", False):
            stats_result = run_cmd(["docker", "stats", "--no-stream", "--format", "json"], timeout=STATS_TIMEOUT)
            if stats_result.get("rc") == 0:
                stats = parse_docker_stats(stats_result.get("out") or "")
        projects = {}
        for container in containers:
            project_name = container.get("project")
            if not project_name:
                continue
            if requested and project_name not in requested:
                continue
            projects.setdefault(project_name, []).append(container)
        for project_name in sorted(projects):
            project_containers = projects[project_name]
            for container in project_containers:
                container["ports"] = _container_ports(container)
                stat = stats.get(container.get("name")) or stats.get(container.get("short_id"))
                container["stats"] = stat
            project_containers.sort(key=_service_sort_key)
            project = {
                "name": project_name,
                "containers": project_containers,
                "emitter": None,
                "kafka": None,
                "flink": None,
            }
            try:
                project["emitter"] = _collect_emitter(project_containers)
            except Exception as exc:
                project["emitter"] = {"available": False, "error": short_reason(exc)}
            try:
                project["kafka"] = _collect_kafka(project_containers, project_name, state, now)
            except Exception as exc:
                project["kafka"] = {"available": False, "error": short_reason(exc)}
            try:
                project["flink"] = _collect_flink(project_containers, project_name, state, now)
            except Exception as exc:
                project["flink"] = {"available": False, "error": short_reason(exc)}
            snapshot["projects"].append(project)
        try:
            snapshot["artifacts"] = _collect_artifacts(getattr(args, "watch_dirs", []), state, now)
        except Exception as exc:
            snapshot["artifacts"] = {"dirs": [], "files": [], "errors": [short_reason(exc)]}
        try:
            snapshot["host"] = _collect_host(getattr(args, "watch_dirs", []))
        except Exception as exc:
            snapshot["host"] = {"load": None, "filesystems": [], "tmpfs": None, "error": short_reason(exc)}
        return snapshot
    except Exception as exc:
        snapshot["warnings"].append(f"dashboard error: {short_reason(exc)}")
        return snapshot


# ---------------------------------------------------------------------------
# plain-text renderer
# ---------------------------------------------------------------------------

def _render_ports(container):
    mappings = container.get("ports") or []
    if not mappings:
        return "none published"
    return ", ".join(
        f"{mapping['container_port']}/{mapping['protocol']}->{mapping['host_ip']}:{mapping['host_port']}"
        for mapping in mappings
    )


def _uptime_label(container):
    if container.get("state") != "running":
        return "n/a"
    status = str(container.get("status") or "")
    if status.startswith("Up "):
        text = status[3:]
        return re.sub(r"\s*\([^)]*\)\s*$", "", text).strip()
    return status or "n/a"


def _render_container_line(container):
    stats = container.get("stats") or {}
    cpu = stats.get("cpu_percent")
    mem = stats.get("mem_usage")
    health = container.get("health") or "none"
    uptime = _uptime_label(container)
    parts = [
        f"{container.get('service') or '?':<16}",
        f"{container.get('short_id') or '?':<12}",
        f"{container.get('image') or '?':<32}",
        f"{container.get('status') or '?':<22}",
        f"health={health}",
        f"uptime={uptime}",
        f"ports={_render_ports(container)}",
    ]
    if cpu is not None:
        parts.append(f"cpu={float(cpu):.1f}%")
    if mem:
        parts.append(f"mem={mem}")
    return "  " + "  ".join(parts)


def _render_emitter(emitter):
    if not emitter or not emitter.get("available"):
        return ["  unavailable: " + short_reason((emitter or {}).get("error") or "emitter unavailable")]
    progress = emitter.get("progress") or {}
    view = derive_emitter_view(progress)
    lines = []
    if view.get("topic"):
        lines.append(f"  topic: {view['topic']}  bootstrap: {view.get('bootstrap') or '?'}  speed: {view.get('speed_multiplier')}x")
    if view.get("finished"):
        lines.append("  state: COMPLETE (Done/Total seen)")
        lines.append("  current file: none (run complete)")
    elif view.get("current_file"):
        lines.append(f"  state: running  current file: {view['current_file']}")
    else:
        lines.append("  state: unknown (no recognizable replay lines in log tail)")
    if view.get("current_file"):
        percent = view.get("current_file_percent")
        label = "last file percent" if view.get("finished") else "current file percent"
        if percent is None:
            lines.append(f"  {label}: unknown (read count not printed until the file finishes)")
        else:
            lines.append(f"  {label}: {percent:.2f}%")
    if view.get("rate_ev_s") is not None:
        lines.append(f"  rate: {format_int(view['rate_ev_s'])} ev/s ({view.get('rate_source')})")
    else:
        lines.append("  rate: unknown")
    if view.get("run_percent") is not None:
        lines.append(f"  run percent: {view['run_percent']:.2f}% (estimate)")
    else:
        lines.append("  run percent: unknown (expected total not known)")
    if view.get("eta_current_s") is not None:
        lines.append(f"  ETA current file: {format_eta(view['eta_current_s'])} (estimate)")
    else:
        lines.append("  ETA current file: unknown")
    if view.get("eta_total_s") is not None:
        lines.append(f"  ETA whole run: {format_eta(view['eta_total_s'])} (estimate)")
    else:
        lines.append("  ETA whole run: unknown")
    if view.get("expected_total") is not None:
        source = view.get("expected_total_source") or "unknown"
        suffix = "" if view.get("expected_total_complete") else " (lower bound; run not complete)"
        lines.append(f"  expected total: {format_int(view['expected_total'])} ({source}){suffix}")
    total = view.get("total")
    if total:
        lines.append(
            f"  Total: read {format_int(total.get('read'))} -> emitted {format_int(total.get('emitted'))} "
            f"({total.get('drop_percent')}% dropped)"
        )
    done = view.get("done_stats")
    if done:
        lines.append(
            f"  Done: {format_int(done.get('submitted'))} submitted, {format_int(done.get('failed'))} failed, "
            f"{format_int(done.get('rate_ev_s'))} ev/s, first paced time {done.get('first_paced_time')}"
        )
    for entry in view.get("files") or []:
        if entry.get("completed"):
            lines.append(
                f"  {entry['name']}: read {format_int(entry.get('read'))} -> emitted {format_int(entry.get('emitted'))}"
            )
    return lines


def _render_kafka(kafka):
    if not kafka or not kafka.get("available"):
        return ["  unavailable: " + short_reason((kafka or {}).get("error") or "kafka unavailable")]
    lines = [f"  bootstrap: {kafka.get('bootstrap')}"]
    topics = kafka.get("topics") or []
    if not topics:
        lines.append("  topics: none matching the project")
    else:
        lines.append("  topics: " + ", ".join(topics))
    totals = kafka.get("totals") or {}
    deltas = kafka.get("deltas") or {}
    for topic in topics:
        partitions = (kafka.get("offsets") or {}).get(topic) or {}
        if not partitions:
            lines.append(f"  {topic}: no offsets")
            continue
        delta_parts = []
        for partition in sorted(partitions):
            delta = ((deltas.get(topic) or {}).get(partition)) if deltas else None
            delta_text = "n/a" if delta is None else f"{delta:+d}"
            delta_parts.append(f"p{partition}={format_int(partitions[partition])} ({delta_text})")
        lines.append(f"  {topic}: total={format_int(totals.get(topic))}  " + "  ".join(delta_parts))
    groups = kafka.get("groups") or []
    if not groups:
        lines.append("  consumer group: unknown (no matching group)")
    for group in groups:
        if not group.get("available", True):
            lines.append(f"  consumer group {group.get('name')}: unknown ({group.get('note') or 'unavailable'})")
            continue
        if group.get("exists") is False:
            lines.append(f"  consumer group {group.get('name')}: unknown (not found)")
            continue
        active = "yes" if group.get("active") else ("no" if group.get("active") is False else "unknown")
        lag = group.get("total_lag")
        lag_text = "unknown" if lag is None else format_int(lag)
        lines.append(
            f"  consumer group {group.get('name')}: active={active}  total lag={lag_text}  "
            f"partitions={len(group.get('partitions') or [])}"
        )
    for error in kafka.get("errors") or []:
        lines.append("  warning: " + error)
    return lines


def _render_flink(flink):
    if not flink or not flink.get("available"):
        return ["  unavailable: " + short_reason((flink or {}).get("error") or "flink unavailable")]
    summary = flink.get("summary") or {}
    lines = []
    if flink.get("stale"):
        seen = flink.get("last_seen_text") or format_epoch(flink.get("last_seen"))
        lines.append(f"  stale: last observed {seen} ({flink.get('error')})")
    if not summary.get("name"):
        lines.append("  no jobs currently")
        return lines
    start = format_epoch(float(summary.get("start_time")) / 1000.0) if summary.get("start_time") else "unknown"
    duration = format_eta(float(summary.get("duration")) / 1000.0) if summary.get("duration") else "unknown"
    lines.append(f"  job: {summary.get('name')}  state: {summary.get('state')}  start: {start}  duration: {duration}")
    for vertex in summary.get("vertices") or []:
        lines.append(
            f"  vertex: {vertex.get('name')}  status={vertex.get('status')}  "
            f"parallelism={vertex.get('parallelism')}  in={format_int(vertex.get('num_records_in'))}  "
            f"out={format_int(vertex.get('num_records_out'))}  backpressure={vertex.get('backpressure')}"
        )
    return lines


def _render_artifacts(artifacts):
    if not artifacts:
        return ["  unavailable: no watch directories"]
    lines = []
    for directory in artifacts.get("dirs") or []:
        if not directory.get("available"):
            lines.append(f"  {directory.get('path')}: unavailable: {directory.get('error')}")
            continue
        lines.append(f"  {directory.get('path')}:")
        found = False
        for item in artifacts.get("files") or []:
            if not item.get("path", "").startswith(directory.get("path", "") + os.sep):
                continue
            found = True
            lines_text = "size only"
            if item.get("lines") is not None:
                lines_text = f"{format_int(item['lines'])} lines"
            growth = item.get("growth_bps")
            growth_text = "n/a" if growth is None else f"{growth:+.1f} B/s"
            try:
                shown = os.path.relpath(item["path"], directory.get("path"))
            except ValueError:
                shown = item["path"]
            lines.append(f"    {shown}  size={format_bytes(item['size'])}  {lines_text}  growth={growth_text}")
        if not found:
            lines.append("    no matching artifact files")
        row = _fs_usage(directory.get("path"))
        if row:
            lines.append(f"    df: {row['used']}/{row['size']} ({row['percent']}%) on {row['target']}")
    for error in artifacts.get("errors") or []:
        lines.append("  warning: " + error)
    return lines


def _render_host(host):
    if not host:
        return "host: unavailable"
    parts = []
    load = host.get("load")
    if load:
        parts.append("load " + " ".join(f"{value:.2f}" for value in load))
    for row in host.get("filesystems") or []:
        parts.append(f"{row['target']} {row['used']}/{row['size']} ({row['percent']}%)")
    tmpfs = host.get("tmpfs")
    if tmpfs:
        parts.append(f"/tmp tmpfs {tmpfs['used']}/{tmpfs['size']} ({tmpfs['percent']}%)")
    if host.get("error"):
        parts.append("error=" + short_reason(host["error"]))
    return "host: " + (" | ".join(parts) if parts else "unavailable")


def render_plain_text(snapshot, include_header=True, width=None):
    """Render a snapshot as plain text.  Pure and defensive."""
    snapshot = snapshot or {}
    lines = []
    docker_error = snapshot.get("docker_error")
    if docker_error:
        lines.append("docker unavailable: " + short_reason(docker_error))
        return "\n".join(truncate_line(line, width) for line in lines)
    projects = snapshot.get("projects") or []
    if include_header and projects:
        lines.append(
            f"ultrastonk stack dashboard  {format_epoch(snapshot.get('timestamp'))}  projects: {len(projects)}"
        )
    if not projects:
        lines.append("no Compose stacks running")
        requested = snapshot.get("requested") or []
        if requested:
            lines.append("requested project(s) not running: " + ", ".join(requested))
        host_line = _render_host(snapshot.get("host"))
        lines.append(host_line)
        return "\n".join(truncate_line(line, width) for line in lines)
    for project in projects:
        try:
            lines.append("")
            lines.append(f"=== project {project.get('name')} ===")
            lines.append("[Containers]")
            for container in project.get("containers") or []:
                lines.append(_render_container_line(container))
            lines.append("[Replay]")
            lines.extend(_render_emitter(project.get("emitter")))
            lines.append("[Kafka]")
            lines.extend(_render_kafka(project.get("kafka")))
            lines.append("[Flink]")
            lines.extend(_render_flink(project.get("flink")))
            lines.append("[Artifacts]")
            lines.extend(_render_artifacts(snapshot.get("artifacts")))
        except Exception as exc:
            lines.append("  unavailable: " + short_reason(exc))
    lines.append("")
    lines.append(_render_host(snapshot.get("host")))
    for warning in snapshot.get("warnings") or []:
        lines.append("warning: " + warning)
    return "\n".join(truncate_line(line, width) for line in lines)


# ---------------------------------------------------------------------------
# curses UI
# ---------------------------------------------------------------------------

def _safe_addstr(stdscr, row, col, text, attr=0):
    try:
        stdscr.addstr(row, col, text, attr)
    except curses.error:
        pass


def _screen_size(stdscr):
    try:
        height, width = stdscr.getmaxyx()
        return max(1, height), max(1, width)
    except curses.error:
        return 24, 80


def _render_header(snapshot, paused, help_on, interval):
    projects = (snapshot or {}).get("projects") or []
    stamp = format_epoch((snapshot or {}).get("timestamp"))
    state = "PAUSED" if paused else "live"
    header = f"ultrastonk stack dashboard  {stamp}  projects: {len(projects)}  {state}  interval: {interval:g}s"
    if help_on:
        header += "  |  q quit  r refresh  p pause  ? help"
    return header


def _draw(stdscr, snapshot, paused, help_on, interval, last_error=None, scroll=0):
    height, width = _screen_size(stdscr)
    try:
        stdscr.erase()
    except curses.error:
        pass
    if height < 4 or width < 20:
        _safe_addstr(stdscr, 0, 0, truncate_line("terminal too small", width), 0)
        try:
            stdscr.refresh()
        except curses.error:
            pass
        return 0
    lines = [_render_header(snapshot, paused, help_on, interval)]
    if help_on:
        lines.append("keys: q quit  r refresh  p pause  ? help  up/down scroll")
    if last_error:
        lines.append("last refresh error: " + short_reason(last_error))
    body = render_plain_text(snapshot, include_header=False, width=width)
    lines.extend(body.splitlines())
    max_scroll = max(0, len(lines) - height)
    scroll = max(0, min(int(scroll), max_scroll))
    for row, line in enumerate(lines[scroll:scroll + height]):
        _safe_addstr(stdscr, row, 0, truncate_line(line, width), 0)
    if len(lines) > height:
        hint = "  lines %d-%d of %d" % (scroll + 1, min(scroll + height, len(lines)), len(lines))
        _safe_addstr(stdscr, height - 1, 0, truncate_line(hint, width), 0)
    try:
        stdscr.refresh()
    except curses.error:
        pass
    return scroll


def _tui_loop(stdscr, args, state):
    try:
        curses.curs_set(0)
    except curses.error:
        pass
    try:
        stdscr.keypad(True)
    except curses.error:
        pass
    try:
        stdscr.timeout(200)
    except curses.error:
        pass
    paused = False
    help_on = False
    scroll = 0
    snapshot = None
    next_refresh = 0.0
    last_error = None
    while True:
        now = time.time()
        if snapshot is None or (not paused and now >= next_refresh):
            try:
                snapshot = collect_snapshot(args, state)
                last_error = None
            except Exception as exc:
                last_error = short_reason(exc)
                if snapshot is None:
                    snapshot = {
                        "timestamp": now,
                        "projects": [],
                        "docker_error": None,
                        "artifacts": None,
                        "host": None,
                        "requested": list(getattr(args, "project", []) or []),
                        "warnings": [],
                    }
            next_refresh = time.time() + max(0.2, float(getattr(args, "interval", 2.0)))
        try:
            scroll = _draw(stdscr, snapshot, paused, help_on, float(getattr(args, "interval", 2.0)), last_error, scroll)
        except Exception as exc:
            try:
                stdscr.erase()
                _safe_addstr(stdscr, 0, 0, truncate_line("render error: " + short_reason(exc), _screen_size(stdscr)[1]), 0)
                stdscr.refresh()
            except Exception:
                pass
        try:
            key = stdscr.getch()
        except curses.error:
            key = -1
        if key in (ord("q"), ord("Q")):
            break
        if key in (ord("r"), ord("R")):
            next_refresh = 0.0
            continue
        if key in (ord("p"), ord("P")):
            paused = not paused
            if not paused:
                next_refresh = 0.0
            continue
        if key == ord("?"):
            help_on = not help_on
            continue
        if key in (curses.KEY_DOWN, ord("j")):
            scroll += 1
            continue
        if key in (curses.KEY_UP, ord("k")):
            scroll = max(0, scroll - 1)
            continue
        if key == curses.KEY_PPAGE:
            scroll = max(0, scroll - 10)
            continue
        if key == curses.KEY_NPAGE:
            scroll += 10
            continue
        if key == curses.KEY_HOME:
            scroll = 0
            continue
        if key == curses.KEY_END:
            scroll = 10 ** 9
            continue
        if key == curses.KEY_RESIZE:
            next_refresh = 0.0


def run_tui(args, state):
    if not sys.stdout.isatty():
        print("interactive mode requires a terminal; use --once for a plain snapshot", file=sys.stderr)
        return 1
    try:
        curses.wrapper(_tui_loop, args, state)
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        print("interactive dashboard unavailable: " + short_reason(exc) + "; use --once", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        description="Read-only terminal dashboard for Ultrastonk Docker Compose experiment stacks.",
        epilog=(
            "examples:\n"
            "  python3 scripts/stack_tui.py --once\n"
            "  python3 scripts/stack_tui.py --once --project ultrastonk-fullweek-2\n"
            "  python3 scripts/stack_tui.py --once --watch /mnt/shared/data/ultrastonk-full-week\n"
            "  python3 scripts/stack_tui.py --interval 3 --project ultrastonk-fullweek-2\n"
            "  python3 scripts/stack_tui.py --once --no-stats --watch /tmp\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project", action="append", default=[], metavar="NAME", help="focus on a Compose project (repeatable)")
    parser.add_argument("--interval", type=float, default=2.0, metavar="SECONDS", help="auto-refresh interval (default: 2)")
    parser.add_argument("--watch", action="append", default=[], metavar="DIR", help="watch a directory for evidence files (repeatable)")
    parser.add_argument("--once", action="store_true", help="print one plain-text snapshot and exit 0")
    parser.add_argument("--no-stats", action="store_true", help="skip docker stats")
    return parser


def resolve_watch_dirs(args):
    dirs = list(getattr(args, "watch", []) or [])
    if not dirs and os.path.isdir(DEFAULT_WATCH):
        dirs.append(DEFAULT_WATCH)
    result = []
    for directory in dirs:
        path = os.path.abspath(os.path.expanduser(directory))
        if path not in result:
            result.append(path)
    return result


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.interval <= 0:
        parser.error("--interval must be greater than 0")
    args.watch_dirs = resolve_watch_dirs(args)
    state = {}
    if args.once:
        try:
            snapshot = collect_snapshot(args, state)
        except Exception as exc:
            snapshot = {
                "timestamp": time.time(),
                "projects": [],
                "docker_error": None,
                "artifacts": None,
                "host": None,
                "requested": list(args.project),
                "warnings": ["dashboard error: " + short_reason(exc)],
            }
        try:
            print(render_plain_text(snapshot))
        except Exception as exc:
            print("dashboard unavailable: " + short_reason(exc))
        return 0
    return run_tui(args, state)


if __name__ == "__main__":
    sys.exit(main())
