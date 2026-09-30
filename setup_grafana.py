"""One-time Grafana setup: register MySQL datasource + import dashboard."""
import urllib.request
import urllib.error
import json
import base64

BASE = "http://localhost:3000"
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()


def call(method, path, data=None):
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(
        BASE + path, data=body,
        headers={"Authorization": AUTH, "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            text = r.read().decode() or "{}"
            return r.status, json.loads(text)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]


# 1. MySQL datasource (uid must match railway-safety.json panels)
ds = {
    "name": "MySQL",
    "uid": "mysql-railway",
    "type": "mysql",
    "access": "proxy",
    "url": "localhost:3306",
    "user": "root",
    "isDefault": True,
    "editable": True,
    "jsonData": {
        "database": "railway_safety",
        "maxOpenConns": 10,
        "connMaxLifetime": 14400,
        "tlsSkipVerify": True,
    },
    "secureJsonData": {"password": "root"},
}
status, body = call("GET", "/api/datasources/uid/mysql-railway")
print("datasource check:", status)
if status == 200:
    ds["id"] = body["id"]
    ds["version"] = body.get("version", 1)
    status, body = call("PUT", "/api/datasources/" + str(ds["id"]), ds)
    print("datasource update:", status, str(body)[:200])
else:
    status, body = call("POST", "/api/datasources", ds)
    print("datasource create:", status, str(body)[:200])

# 2. Import dashboard
with open("grafana/dashboards/railway-safety.json", encoding="utf-8") as f:
    dash = json.load(f)
dash["id"] = None
status, body = call(
    "POST", "/api/dashboards/db",
    {"dashboard": dash, "overwrite": True, "folderId": 0},
)
print("dashboard import:", status, str(body)[:250])
