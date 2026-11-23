#!/usr/bin/env python3
"""Read-only smoke check for a running Flink and monitoring stack."""
import argparse
import json
import os
import time
from collections import Counter
import urllib.parse
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)


def require(condition, message):
    if not condition:
        raise SystemExit(f"Monitoring check failed: {message}")


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def query(url, path, **params):
    response = get(url + path + "?" + urllib.parse.urlencode(params))
    require(response.get("status") == "success", f"{path}: {response}")
    return response["data"]["result"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--grafana", default="http://localhost:3000")
    parser.add_argument("--flink", default="http://localhost:8081")
    parser.add_argument("--project", default=os.getenv("COMPOSE_PROJECT_NAME", "ultrastonk"))
    parser.add_argument("--expected-taskmanagers", type=positive_int,
                        default=os.getenv("EXPECTED_TASKMANAGERS", "2"),
                        help="minimum worker count; must match the resource-exporter setting (default 2)")
    parser.add_argument("--resource-max-age", type=positive_int, default=30,
                        help="maximum resource sample age in seconds (default 30)")
    parser.add_argument("--log-max-age", type=positive_int, default=120,
                        help="maximum Flink log age in seconds (default 120); idle stacks may fail")
    parser.add_argument("--log-contains", help="require a literal string in the recent Flink log, e.g. a known marker")
    args = parser.parse_args()
    targets = get(args.prometheus + "/api/v1/targets")["data"]["activeTargets"]
    minimum = {"flink-jobmanager": 1, "flink-taskmanagers": args.expected_taskmanagers,
               "kafka": 1, "loki": 1, "resources": 1}
    required = [t for t in targets if t["labels"]["job"] in minimum]
    healthy = Counter(t["labels"]["job"] for t in required if t["health"] == "up")
    for job, count in minimum.items():
        require(healthy[job] >= count, f"{job}: {healthy[job]} healthy scrape targets, expected at least {count}")
    down = [(t["labels"], t["lastError"]) for t in required if t["health"] != "up"]
    require(not down, f"unhealthy required targets: {down}")
    registered = len(get(args.flink + "/taskmanagers")["taskmanagers"])
    require(registered >= args.expected_taskmanagers,
            f"{registered} registered TaskManagers, expected at least {args.expected_taskmanagers}")

    def promql(expression):
        return query(args.prometheus, "/api/v1/query", query=expression)

    expected = promql('pipeline_expected_taskmanagers{job="resources"}')
    require(expected and all(float(s["value"][1]) == args.expected_taskmanagers for s in expected),
            "expected TaskManager count missing or inconsistent with resource-exporter configuration")
    errors = promql('pipeline_resource_collection_errors{job="resources"}')
    require(errors and all(float(s["value"][1]) == 0 for s in errors), "missing or failed resource collection")
    project = json.dumps(args.project)
    samples = promql(f'time() - pipeline_container_sample_timestamp_seconds{{job="resources",project={project}}}'
                     f' <= {args.resource_max_age}')
    for service, count in {"jobmanager": 1, "taskmanager": args.expected_taskmanagers, "kafka": 1}.items():
        containers = {s["metric"]["container_id"] for s in samples if s["metric"]["service"] == service}
        require(len(containers) >= count,
                f"{service}: {len(containers)} fresh resource samples for project {args.project}, expected at least {count}")

    require(get(args.grafana + "/api/health")["database"] == "ok", "Grafana database unhealthy")
    dashboards = []
    for uid in ("ultrastonk", "ultrastonk-profiling"):
        dashboard = get(args.grafana + "/api/dashboards/uid/" + uid)["dashboard"]
        require(dashboard["panels"], f"Grafana dashboard {uid} has no panels")
        dashboards.append(dashboard["title"])
    log_query = f'{{project={project},service=~"jobmanager|taskmanager"}}'
    if args.log_contains:
        log_query += " |= " + json.dumps(args.log_contains)
    end = time.time_ns()
    start = end - args.log_max_age * 1_000_000_000
    logs = query(args.grafana, "/api/datasources/proxy/uid/loki/loki/api/v1/query_range",
                 query=log_query, limit=5, start=start, end=end)
    require(any(start <= int(timestamp) <= end for stream in logs for timestamp, _ in stream["values"]),
            f"no matching Flink log in the last {args.log_max_age}s for project {args.project}")
    print(json.dumps({"monitoring_passed": True, "healthy_required_targets": len(required),
                      "registered_taskmanagers": registered, "dashboards": dashboards, "log_streams": len(logs)}, indent=2))


if __name__ == "__main__":
    main()
