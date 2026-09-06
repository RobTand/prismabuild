#!/usr/bin/env python3
"""Install this dashboard through Grafana's API using an existing credential.

The credential file is JSON: {"token": "..."} or
{"username": "...", "password": "..."}. Credentials are never printed.
An existing, healthy Prometheus datasource must already contain PB metrics.
"""
import argparse
import base64
import json
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request


class Grafana:
    def __init__(self, url, auth):
        self.url = url.rstrip("/")
        if urllib.parse.urlsplit(self.url).scheme not in ("http", "https"):
            raise ValueError("Grafana URL must use HTTP or HTTPS")
        if auth.get("token"):
            self.authorization = "Bearer " + auth["token"]
        else:
            raw = (auth["username"] + ":" + auth["password"]).encode()
            self.authorization = "Basic " + base64.b64encode(raw).decode()

    def request(self, path, body=None, missing_ok=False):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.url + path, data=data, headers={
            "Authorization": self.authorization, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if missing_ok and exc.code == 404:
                return None
            # Do not log request headers or arbitrary server response bodies.
            raise RuntimeError(f"Grafana {path}: HTTP {exc.code}") from None


def deploy(client, dashboard, datasource_uid, backup_dir):
    uid = dashboard["uid"]
    datasource = client.request("/api/datasources/uid/" + urllib.parse.quote(datasource_uid, safe=""))
    if datasource.get("type") != "prometheus":
        raise ValueError("Selected datasource is not Prometheus")
    health = client.request("/api/datasources/uid/" + urllib.parse.quote(datasource_uid, safe="") + "/health")
    if health.get("status", "").lower() != "ok":
        raise ValueError("Selected datasource health check is not OK")
    existing = client.request("/api/dashboards/uid/" + uid, missing_ok=True)
    if existing:
        if "prismabuild" not in existing["dashboard"].get("tags", []):
            raise ValueError("Dashboard UID is already used by an unrelated dashboard")
        backup_dir.mkdir(parents=True, exist_ok=True)
        version = existing["dashboard"]["version"]
        backup = backup_dir / f"{uid}-v{version}.json"
        if not backup.exists():
            with backup.open("x") as out:
                json.dump(existing, out, indent=2)
                out.write("\n")
        dashboard["id"] = existing["dashboard"]["id"]
        dashboard["version"] = version
        folder_uid = existing["meta"].get("folderUid", "")
    else:
        folder_uid = "prismabuild"
        folder = client.request("/api/folders/" + folder_uid, missing_ok=True)
        if not folder:
            client.request("/api/folders", {"uid": folder_uid, "title": "PrismaBuild"})
    dashboard["templating"]["list"][0]["current"] = {
        "text": datasource["name"], "value": datasource_uid}
    # Grafana's version comparison refuses a concurrent edit rather than
    # overwriting it. Updates preserve the existing folder and save a backup.
    result = client.request("/api/dashboards/db", {
        "dashboard": dashboard, "folderUid": folder_uid, "overwrite": False,
        "message": "Deploy versioned PrismaBuild fleet dashboard"})
    if result.get("status") != "success":
        raise RuntimeError("Grafana did not confirm dashboard creation")
    installed = client.request("/api/dashboards/uid/" + uid)
    if installed["dashboard"]["title"] != dashboard["title"]:
        raise RuntimeError("Grafana readback did not match the dashboard title")
    return {"uid": uid, "version": result.get("version"), "url": client.url + result["url"],
            "panels": len(installed["dashboard"]["panels"]), "datasource_uid": datasource_uid}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://192.168.1.120:3000")
    parser.add_argument("--auth-file", type=Path, required=True)
    parser.add_argument("--datasource-uid", required=True)
    parser.add_argument("--dashboard", type=Path, default=Path(__file__).with_name("prismabuild.json"))
    parser.add_argument("--backup-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = deploy(Grafana(args.url, json.loads(args.auth_file.read_text())),
                        json.loads(args.dashboard.read_text()), args.datasource_uid, args.backup_dir)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        parser.exit(1, f"dashboard deployment: {exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
