#!/usr/bin/env python3
"""Read-only smoke check for the published monitoring endpoints and stored Flink logs."""
import argparse
import json
import urllib.parse
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--grafana", default="http://localhost:3000")
    parser.add_argument("--project", default="ultrastonk")
    args = parser.parse_args()
    targets = get(args.prometheus + "/api/v1/targets")["data"]["activeTargets"]
    required = [target for target in targets if target["labels"]["job"] in {"flink-jobmanager", "flink-taskmanagers", "kafka"}]
    assert any(t["labels"]["job"] == "flink-jobmanager" for t in required), "missing JobManager target"
    assert any(t["labels"]["job"] == "flink-taskmanagers" for t in required), "missing TaskManager targets"
    assert any(t["labels"]["job"] == "kafka" for t in required), "missing Kafka target"
    assert all(t["health"] == "up" for t in required), [(t["labels"], t["lastError"]) for t in required]
    assert get(args.grafana + "/api/health")["database"] == "ok"
    dashboard = get(args.grafana + "/api/dashboards/uid/ultrastonk")
    assert dashboard["dashboard"]["panels"], "dashboard missing"
    query = '{project="' + args.project + '",service=~"jobmanager|taskmanager"}'
    params = urllib.parse.urlencode({"query": query, "limit": 5, "since": "15m"})
    logs = get(args.grafana + "/api/datasources/proxy/uid/loki/loki/api/v1/query_range?" + params)
    assert logs["data"]["result"], "no Flink logs found in Loki"
    print(json.dumps({"monitoring_passed": True, "healthy_required_targets": len(required),
                      "dashboard": dashboard["dashboard"]["title"], "log_streams": len(logs["data"]["result"])}, indent=2))


if __name__ == "__main__":
    main()
