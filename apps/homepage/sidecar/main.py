import asyncio
import httpx
import os
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

PROMETHEUS_URL  = os.getenv("PROMETHEUS_URL",  "http://kube-prometheus-stack-prometheus.monitoring:9090/prometheus")
GLITCHTIP_URL   = os.getenv("GLITCHTIP_URL",   "http://glitchtip-web.glitchtip:8000")
GLITCHTIP_TOKEN = os.getenv("GLITCHTIP_TOKEN", "")

# Namespaces to skip for auto-discovery
SKIP_NAMESPACES = {"kube-system", "nginx-ingress", "registry", "homepage"}

# Display name overrides
NAME_OVERRIDES = {
    "vaultwarden.th3borg.org": "VAULTWARDEN",
    "bookstack.th3borg.org":   "BOOKSTACK",
    "grafana.th3borg.org":     "GRAFANA",
    "glitchtip.th3borg.org":   "GLITCHTIP",
    "argocd.th3borg.org":      "ARGOCD",
    "uptime.th3borg.org":      "UPTIME KUMA",
    "livesync.th3borg.org":    "LIVESYNC",
    "kavita.th3borg.org":      "KAVITA",
    "prometheus.th3borg.org":  "PROMETHEUS",
    "th3borg.org":             "HOMEPAGE",
}

K8S_HOST = "https://kubernetes.default.svc"
SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_CA_PATH    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def k8s_token():
    try:
        with open(SA_TOKEN_PATH) as f:
            return f.read().strip()
    except Exception:
        return None


async def get_ingresses() -> list:
    token = k8s_token()
    if not token:
        return []
    try:
        async with httpx.AsyncClient(verify=SA_CA_PATH) as client:
            r = await client.get(
                f"{K8S_HOST}/apis/networking.k8s.io/v1/ingresses",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            services = []
            for item in data.get("items", []):
                ns = item["metadata"]["namespace"]
                if ns in SKIP_NAMESPACES:
                    continue
                for rule in item.get("spec", {}).get("rules", []):
                    host = rule.get("host", "")
                    if not host or not host.endswith("th3borg.org"):
                        continue
                    # Get internal service from ingress backend
                    paths = rule.get("http", {}).get("paths", [])
                    if not paths:
                        continue
                    backend = paths[0].get("backend", {}).get("service", {})
                    svc_name = backend.get("name", "")
                    svc_port = backend.get("port", {}).get("number", 80)
                    internal = f"http://{svc_name}.{ns}:{svc_port}"
                    display = NAME_OVERRIDES.get(host, host.split(".")[0].upper())
                    services.append({
                        "name": display,
                        "url": f"https://{host}",
                        "internal": internal,
                    })
            # Sort by display name
            services.sort(key=lambda x: x["name"])
            return services
    except Exception as e:
        print(f"k8s ingress discovery error: {e}")
        return []


async def check_service(client: httpx.AsyncClient, svc: dict) -> dict:
    import time
    start = time.monotonic()
    try:
        r = await client.get(svc["internal"], timeout=5.0, follow_redirects=True)
        ping = int((time.monotonic() - start) * 1000)
        status = "up" if r.status_code < 500 else "down"
        return {**svc, "status": status, "ping": ping, "code": r.status_code}
    except Exception as e:
        return {**svc, "status": "down", "ping": None, "code": None, "error": str(e)}


async def fetch_prometheus() -> dict:
    queries = {
        "cpu":       'query?query=100-(avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)',
        "mem_total": "query?query=node_memory_MemTotal_bytes",
        "mem_avail": "query?query=node_memory_MemAvailable_bytes",
        "pods":      'query?query=count(kube_pod_info{namespace!="kube-system"})',
        "uptime":    "query?query=node_time_seconds-node_boot_time_seconds",
    }
    results = {}
    async with httpx.AsyncClient() as client:
        for key, q in queries.items():
            try:
                r = await client.get(f"{PROMETHEUS_URL}/api/v1/{q}", timeout=3.0)
                data = r.json()
                val = data["data"]["result"][0]["value"][1]
                results[key] = float(val)
            except Exception:
                results[key] = None
    return results


async def fetch_glitchtip_issues() -> list:
    if not GLITCHTIP_TOKEN:
        return []
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{GLITCHTIP_URL}/api/0/issues/?limit=8",
                headers={"Authorization": f"Bearer {GLITCHTIP_TOKEN}"},
                timeout=5.0,
            )
            if r.status_code == 200:
                return [
                    {
                        "title":    i.get("title") or i.get("culprit", "Unknown"),
                        "project":  i.get("project", {}).get("name", "unknown"),
                        "count":    i.get("count", 0),
                        "level":    i.get("level", "error"),
                        "lastSeen": i.get("lastSeen", ""),
                    }
                    for i in r.json()
                ]
    except Exception:
        pass
    return []


@app.get("/api/status")
async def status():
    services = await get_ingresses()
    async with httpx.AsyncClient() as client:
        service_results = await asyncio.gather(*[check_service(client, s) for s in services])

    prom = await fetch_prometheus()
    issues = await fetch_glitchtip_issues()

    mem_total = prom.get("mem_total")
    mem_avail = prom.get("mem_avail")
    mem_used_pct = None
    if mem_total and mem_avail:
        mem_used_pct = round(((mem_total - mem_avail) / mem_total) * 100, 1)

    return {
        "services": list(service_results),
        "cluster": {
            "cpu_pct":      round(prom["cpu"], 1) if prom.get("cpu") else None,
            "mem_total_gb": round(mem_total / 1073741824, 1) if mem_total else None,
            "mem_avail_gb": round(mem_avail / 1073741824, 1) if mem_avail else None,
            "mem_used_pct": mem_used_pct,
            "pods":         int(prom["pods"]) if prom.get("pods") else None,
            "uptime_seconds": int(prom["uptime"]) if prom.get("uptime") else None,
        },
        "errors": issues,
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True}
