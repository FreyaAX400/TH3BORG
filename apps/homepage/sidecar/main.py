import asyncio
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

SERVICES = [
    {"name": "VAULTWARDEN", "url": "https://vaultwarden.th3borg.org", "internal": "http://vaultwarden.vaultwarden:80"},
    {"name": "BOOKSTACK",   "url": "https://bookstack.th3borg.org",   "internal": "http://bookstack.bookstack:80"},
    {"name": "GRAFANA",     "url": "https://grafana.th3borg.org",     "internal": "http://kube-prometheus-stack-grafana.monitoring:80"},
    {"name": "GLITCHTIP",   "url": "https://glitchtip.th3borg.org",   "internal": "http://glitchtip-web.glitchtip:8000"},
    {"name": "ARGOCD",      "url": "https://argocd.th3borg.org",      "internal": "http://argocd-server.argocd:80"},
    {"name": "UPTIME KUMA", "url": "https://uptime.th3borg.org",      "internal": "http://uptime-kuma.monitoring:3001"},
    {"name": "LIVESYNC",    "url": "https://livesync.th3borg.org",    "internal": "http://livesync.livesync:5984"},
    {"name": "KAVITA",      "url": "https://kavita.th3borg.org",      "internal": "http://kavita.kavita:5000"},
]

PROMETHEUS_URL  = os.getenv("PROMETHEUS_URL",  "http://kube-prometheus-stack-prometheus.monitoring:9090/prometheus")
GLITCHTIP_URL   = os.getenv("GLITCHTIP_URL",   "http://glitchtip-web.glitchtip:8000")
GLITCHTIP_TOKEN = os.getenv("GLITCHTIP_TOKEN", "")


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
        "cpu":      'query?query=100-(avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)',
        "mem_total": "query?query=node_memory_MemTotal_bytes",
        "mem_avail": "query?query=node_memory_MemAvailable_bytes",
        "pods":     'query?query=count(kube_pod_info{namespace!="kube-system"})',
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
                issues = r.json()
                return [
                    {
                        "title":   i.get("title") or i.get("culprit", "Unknown"),
                        "project": i.get("project", {}).get("name", "unknown"),
                        "count":   i.get("count", 0),
                        "level":   i.get("level", "error"),
                        "lastSeen": i.get("lastSeen", ""),
                    }
                    for i in issues
                ]
    except Exception:
        pass
    return []


@app.get("/api/status")
async def status():
    async with httpx.AsyncClient() as client:
        service_results = await asyncio.gather(*[check_service(client, s) for s in SERVICES])

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
        },
        "errors": issues,
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True}
