"""Project-scoped Docker resources in capacity-normalized units; no hardware performance counters."""
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
import math
import os
import socket
import threading
import time
import urllib.parse


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=10)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


class Docker:
    def __init__(self, path="/var/run/docker.sock"):
        self.path = path

    def get(self, path):
        connection = UnixConnection(self.path)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            if response.status != 200:
                raise RuntimeError(f"Docker {path}: HTTP {response.status}")
            return json.load(response)
        finally:
            connection.close()


def cpuset_size(value):
    cpus = set()
    for part in value.split(","):
        if not part:
            continue
        bounds = part.split("-")
        first, last = int(bounds[0]), int(bounds[-1])
        if first < 0 or last < first or last - first > 100000:
            raise ValueError("invalid CPU set")
        cpus.update(range(first, last + 1))
    return len(cpus)


def normalize(stats, inspect, host_cpus, host_memory, previous=None, elapsed=None):
    """Missing kernel data stays absent. Ratios are fractions, not percentages."""
    result = {}
    config = inspect.get("HostConfig", {})
    capacities = [float(host_cpus)] if host_cpus and host_cpus > 0 else []
    constrained = False
    if config.get("NanoCpus", 0) > 0:
        capacities.append(config["NanoCpus"] / 1e9)
        constrained = True
    if config.get("CpuQuota", 0) > 0 and config.get("CpuPeriod", 0) > 0:
        capacities.append(config["CpuQuota"] / config["CpuPeriod"])
        constrained = True
    if config.get("CpusetCpus"):
        size = cpuset_size(config["CpusetCpus"])
        if size:
            capacities.append(float(size))
            constrained = True
    capacity = min(capacities) if capacities else None
    if capacity:
        result["cpu_capacity_cores"] = capacity
    result["cpu_has_explicit_limit"] = int(constrained)
    cpu = stats.get("cpu_stats", {})
    usage = cpu.get("cpu_usage", {}).get("total_usage")
    if usage is not None:
        result["cpu_seconds_total"] = usage / 1e9
        old = (previous or {}).get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage")
        if old is not None and usage >= old and elapsed and elapsed > 0:
            cores = (usage - old) / 1e9 / elapsed
            result["cpu_used_cores"] = cores
            if capacity:
                result["cpu_utilization_ratio"] = cores / capacity
    throttling = cpu.get("throttling_data", {})
    old_throttling = (previous or {}).get("cpu_stats", {}).get("throttling_data", {})
    for source, target, factor in [("periods", "cpu_periods_total", 1),
                                   ("throttled_periods", "cpu_throttled_periods_total", 1),
                                   ("throttled_time", "cpu_throttled_seconds_total", 1e-9)]:
        if source in throttling:
            result[target] = throttling[source] * factor
    if all(k in throttling and k in old_throttling for k in ["periods", "throttled_periods"]):
        periods = throttling["periods"] - old_throttling["periods"]
        throttled = throttling["throttled_periods"] - old_throttling["throttled_periods"]
        if periods > 0 and 0 <= throttled <= periods:
            result["cpu_throttled_periods_ratio"] = throttled / periods
    memory = stats.get("memory_stats", {})
    memory_limit = config.get("Memory", 0)
    result["memory_has_explicit_limit"] = int(memory_limit > 0)
    limits = [value for value in [memory.get("limit"), host_memory, memory_limit] if value and value > 0]
    limit = min(limits) if limits else None
    if limit:
        result["memory_capacity_bytes"] = limit
    if "usage" in memory:
        cache = memory.get("stats", {}).get("total_inactive_file", memory.get("stats", {}).get("inactive_file", 0))
        working = max(0, memory["usage"] - cache)
        result["memory_usage_bytes"] = memory["usage"]
        result["memory_working_set_bytes"] = working
        if limit:
            result["memory_utilization_ratio"] = working / limit
    if "networks" in stats:
        for direction in ["rx", "tx"]:
            if all(direction + "_bytes" in net for net in stats["networks"].values()):
                result["network_" + direction + "_bytes_total"] = sum(net[direction + "_bytes"] for net in stats["networks"].values())
    block = stats.get("blkio_stats", {}).get("io_service_bytes_recursive")
    if block is not None:
        for operation in ["read", "write"]:
            result["block_" + operation + "_bytes_total"] = sum(row["value"] for row in block if row["op"].lower() == operation)
    if "current" in stats.get("pids_stats", {}):
        result["processes"] = stats["pids_stats"]["current"]
    return result


def escape(value):
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def exposition(rows, errors):
    lines = ["# TYPE pipeline_resource_collection_errors gauge", f"pipeline_resource_collection_errors {errors}"]
    types = set()
    for labels, values in rows:
        suffix = "{" + ",".join(f'{key}="{escape(value)}"' for key, value in sorted(labels.items())) + "}"
        for name, value in sorted(values.items()):
            if not math.isfinite(value):
                continue
            metric = "pipeline_container_" + name
            if name not in types:
                lines.append(f"# TYPE {metric} " + ("counter" if name.endswith("_total") else "gauge"))
                types.add(name)
            lines.append(f"{metric}{suffix} {value}")
    return "\n".join(lines) + "\n"


class Collector:
    def __init__(self, docker, project):
        self.docker = docker
        self.project = project
        self.previous = {}
        self.lock = threading.Lock()
        self.body = exposition([], 0)

    def collect(self):
        rows, errors, alive = [], 0, set()
        try:
            version = self.docker.get("/version")["ApiVersion"]
            base = "/v" + version
            info = self.docker.get(base + "/info")
            filters = urllib.parse.urlencode({"filters": json.dumps({"label": ["com.docker.compose.project=" + self.project]})})
            containers = self.docker.get(base + "/containers/json?" + filters)
            def sample(container):
                identity = container["Id"]
                labels = {"project": self.project,
                          "service": container["Labels"].get("com.docker.compose.service", "unknown"),
                          "container": container["Names"][0].lstrip("/"), "container_id": identity[:12]}
                try:
                    inspect = self.docker.get(base + "/containers/" + identity + "/json")
                    stats = self.docker.get(base + "/containers/" + identity + "/stats?stream=false&one-shot=true")
                    now = time.monotonic()
                    old, then = self.previous.get(identity, (None, None))
                    values = normalize(stats, inspect, info.get("NCPU"), info.get("MemTotal"), old, now - then if then is not None else None)
                    self.previous[identity] = stats, now
                    values.update({"sample_success": 1, "sample_timestamp_seconds": time.time()})
                    return identity, labels, values
                except Exception as error:
                    print(json.dumps({"event": "resource_sample_error", "container": labels["container"], "error": str(error)}), flush=True)
                    return identity, labels, {"sample_success": 0}
            with ThreadPoolExecutor(max_workers=8) as pool:
                for identity, labels, values in pool.map(sample, containers):
                    alive.add(identity)
                    rows.append((labels, values))
                    errors += 1 - values["sample_success"]
            self.previous = {key: value for key, value in self.previous.items() if key in alive}
        except Exception as error:
            errors += 1
            print(json.dumps({"event": "resource_collection_error", "error": str(error)}), flush=True)
        with self.lock:
            self.body = exposition(rows, errors)


def main():
    collector = Collector(Docker(os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")), os.getenv("COMPOSE_PROJECT", "ultrastonk"))
    interval = float(os.getenv("RESOURCE_SAMPLE_INTERVAL_SECONDS", "5"))
    expected_taskmanagers = int(os.getenv("EXPECTED_TASKMANAGERS", "2"))
    if interval <= 0:
        raise ValueError("RESOURCE_SAMPLE_INTERVAL_SECONDS must be positive")
    if expected_taskmanagers <= 0:
        raise ValueError("EXPECTED_TASKMANAGERS must be positive")
    expected_metric = ("# TYPE pipeline_expected_taskmanagers gauge\n"
                       f"pipeline_expected_taskmanagers {expected_taskmanagers}\n")
    def run():
        while True:
            started = time.monotonic()
            collector.collect()
            time.sleep(max(.1, interval - (time.monotonic() - started)))
    threading.Thread(target=run, daemon=True).start()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            with collector.lock:
                body = (expected_metric + collector.body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass
    ThreadingHTTPServer(("0.0.0.0", int(os.getenv("RESOURCE_EXPORTER_PORT", "8000"))), Handler).serve_forever()


if __name__ == "__main__":
    main()
