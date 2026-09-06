#!/usr/bin/env python3
"""Exercise the real Grafana/Prometheus stack inside an admitted PB action.

Creates temporary loopback-only containers, reads real fleet telemetry, checks
every dashboard expression, and optionally renders a Firefox screenshot. No
production Grafana configuration is changed. Invoke through published pbrun.
"""
import argparse
import base64
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]


def request(url, body=None, auth=None):
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = auth
    req = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def wait_for(url, deadline=90):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        try:
            return request(url)
        except Exception:
            time.sleep(1)
    raise RuntimeError("Endpoint did not become ready: " + url)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--screenshot", action="store_true")
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=True)
    prefix = "pb-grafana-" + secrets.token_hex(5)
    containers = []
    exporter = None
    images = ["prom/prometheus:v3.5.0", "grafana/grafana:12.4.1"]
    report = {"hostname": socket.gethostname(), "started_unix": time.time(), "images": images}
    with tempfile.TemporaryDirectory(prefix=prefix) as directory:
        temp = Path(directory)
        # Docker daemons need to traverse the bind-mounted directory.
        temp.chmod(0o755)
        log = (args.artifacts / "exporter.log").open("w")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                def pull(image):
                    result = subprocess.run(["docker", "pull", image], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    (args.artifacts / (image.split("/")[-1].replace(":", "-") + "-pull.log")).write_text(result.stdout)
                    if result.returncode:
                        raise RuntimeError("Image pull failed: " + image)
                list(ex.map(pull, images))
            with socket.socket() as sock:
                sock.bind(("0.0.0.0", 0))
                exporter_port = sock.getsockname()[1]
            exporter = subprocess.Popen([sys.executable, str(ROOT / "tools/fleet/pbmetrics.py"), "--listen", "0.0.0.0", "--port", str(exporter_port)], stdout=log, stderr=subprocess.STDOUT)
            config = (Path(__file__).with_name("prometheus.scrape.yml")).read_text().replace("192.168.1.180:9877", f"host.docker.internal:{exporter_port}")
            (temp / "prometheus.yml").write_text("global:\n  scrape_interval: 10s\n" + config)
            prometheus_name = prefix + "-prom"
            def run_container(name, image, options, command=()):
                cid = subprocess.check_output(["docker", "run", "--detach", "--rm", "--name", name, *options, image, *command], text=True).strip()
                containers.append(cid)
                return cid
            prom = run_container(prometheus_name, images[0], ["--add-host", "host.docker.internal:host-gateway", "-p", "127.0.0.1::9090", "-v", f"{temp}/prometheus.yml:/etc/prometheus/prometheus.yml:ro"])
            def port(cid, internal):
                address = subprocess.check_output(["docker", "port", cid, str(internal)], text=True).strip().splitlines()[0]
                return "http://" + address
            prometheus_url = port(prom, 9090)
            wait_for(prometheus_url + "/api/v1/status/buildinfo")
            password = secrets.token_urlsafe(24)
            env_path = temp / "grafana.env"
            env_path.write_text("GF_SECURITY_ADMIN_PASSWORD=" + password + "\nGF_USERS_DEFAULT_THEME=dark\nGF_ANALYTICS_REPORTING_ENABLED=false\nGF_ANALYTICS_CHECK_FOR_UPDATES=false\n")
            env_path.chmod(0o600)
            grafana = run_container(prefix + "-grafana", images[1], ["-p", "127.0.0.1::3000", "--link", prometheus_name + ":prometheus", "--env-file", str(env_path)])
            grafana_url = port(grafana, 3000)
            wait_for(grafana_url + "/api/health")
            auth = "Basic " + base64.b64encode(("admin:" + password).encode()).decode()
            request(grafana_url + "/api/datasources", {"uid": "prismabuild-prometheus", "name": "PrismaBuild", "type": "prometheus", "access": "proxy", "url": "http://prometheus:9090", "jsonData": {"httpMethod": "POST", "timeInterval": "10s"}}, auth)
            spec = importlib.util.spec_from_file_location("deploy_dashboard", Path(__file__).with_name("deploy_dashboard.py"))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            dashboard = json.loads(Path(__file__).with_name("prismabuild.json").read_text())
            report["deployment"] = module.deploy(module.Grafana(grafana_url, {"username": "admin", "password": password}), dashboard, "prismabuild-prometheus", temp / "backups")
            # Wait for at least two collection/scrape cycles, not just HTTP up.
            time.sleep(25)
            report["targets"] = [{"job": t["labels"].get("job"), "host": t["labels"].get("host"), "health": t["health"], "error": t["lastError"]} for t in request(prometheus_url + "/api/v1/targets")["data"]["activeTargets"]]
            if not all(t["health"] == "up" for t in report["targets"]):
                raise RuntimeError("A Prometheus scrape target is down")
            report["queries"] = []
            for panel in dashboard["panels"]:
                for target in panel.get("targets", []):
                    expression = target["expr"].replace("$host", ".*")
                    data = request(prometheus_url + "/api/v1/query?" + urllib.parse.urlencode({"query": expression}))
                    if data["status"] != "success":
                        raise RuntimeError("Query failed: " + panel["title"])
                    report["queries"].append({"panel": panel["title"], "expression": expression, "series": len(data["data"]["result"])})
            if args.screenshot:
                venv = temp / "browser-venv"
                subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
                browser_python = str(venv / "bin/python")
                subprocess.run([browser_python, "-m", "pip", "install", "--quiet", "playwright"], check=True)
                subprocess.run([browser_python, "-m", "playwright", "install", "firefox"], check=True)
                browser_script = temp / "render.py"
                browser_script.write_text('''import json,os,sys,time
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
 browser=p.firefox.launch(headless=True)
 context=browser.new_context(viewport={"width":1600,"height":1100},http_credentials={"username":"admin","password":os.environ["PB_PREVIEW_PASSWORD"]})
 page=context.new_page()
 errors=[]
 page.on("pageerror",lambda error:errors.append(str(error)))
 page.goto(sys.argv[1],wait_until="networkidle",timeout=90000)
 page.wait_for_timeout(15000)
 page.screenshot(path=sys.argv[2],full_page=True)
 print(json.dumps({"title":page.title(),"page_errors":errors,"body_excerpt":page.locator("body").inner_text()[:500]}))
 browser.close()
''')
                result = subprocess.run([browser_python, str(browser_script), grafana_url + "/d/prismabuild-fleet?orgId=1&kiosk", str(args.artifacts / "dashboard.png")], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={**os.environ, "PB_PREVIEW_PASSWORD": password})
                (args.artifacts / "browser.stderr").write_text(result.stderr)
                if result.returncode:
                    raise RuntimeError("Browser rendering failed; inspect browser.stderr")
                report["browser"] = json.loads(result.stdout)
            report["success"] = True
        finally:
            for cid in reversed(containers):
                logs = subprocess.run(["docker", "logs", cid], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                (args.artifacts / (cid[:12] + ".log")).write_text(logs.stdout)
                subprocess.run(["docker", "rm", "--force", cid], check=True, stdout=subprocess.DEVNULL)
            if exporter:
                exporter.terminate()
                exporter.wait(timeout=15)
            log.close()
            report["finished_unix"] = time.time()
            (args.artifacts / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
