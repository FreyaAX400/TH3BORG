import asyncio
import httpx
import os
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

app = FastAPI(title="TH3BORG API", version="1.0.0", docs_url=None, redoc_url=None)

def _load_keys():
    keys = {}
    for entry in os.getenv("API_KEYS", "").split(","):
        entry = entry.strip()
        if ":" in entry:
            k, s = entry.split(":", 1)
            keys[k.strip()] = s.strip()
        elif entry:
            keys[entry] = "full"
    return keys

API_KEYS = _load_keys()
security = HTTPBearer()

def require_scope(*scopes):
    def checker(credentials: HTTPAuthorizationCredentials = Security(security)):
        key = credentials.credentials
        if key not in API_KEYS:
            raise HTTPException(status_code=401, detail="Invalid API key")
        key_scope = API_KEYS[key]
        if key_scope == "full":
            return key_scope
        if key_scope not in scopes:
            raise HTTPException(status_code=403, detail="Insufficient scope")
        return key_scope
    return checker

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://th3borg.org",
        "https://control.th3borg.org",
        "https://apps.th3borg.org",
    ],
    allow_methods=["GET"],
    allow_headers=["Authorization"],
)

PROMETHEUS_URL  = os.getenv("PROMETHEUS_URL", "http://kube-prometheus-stack-prometheus.monitoring:9090/prometheus")
GLITCHTIP_URL   = os.getenv("GLITCHTIP_URL",  "http://glitchtip-web.glitchtip:8000")
GLITCHTIP_TOKEN = os.getenv("GLITCHTIP_TOKEN", "")

SKIP_NAMESPACES = {"kube-system", "nginx-ingress", "registry", "homepage", "api"}
APPS_PORTAL_SERVICES = {"kavita.th3borg.org", "vaultwarden.th3borg.org", "bookstack.th3borg.org"}

NAME_OVERRIDES = {
    "vaultwarden.th3borg.org":  "VAULTWARDEN",
    "bookstack.th3borg.org":    "BOOKSTACK",
    "grafana.th3borg.org":      "GRAFANA",
    "glitchtip.th3borg.org":    "GLITCHTIP",
    "argocd.th3borg.org":       "ARGOCD",
    "uptime.th3borg.org":       "UPTIME KUMA",
    "livesync.th3borg.org":     "LIVESYNC",
    "kavita.th3borg.org":       "KAVITA",
    "prometheus.th3borg.org":   "PROMETHEUS",
    "control.th3borg.org":      "CONTROL",
    "lldap.th3borg.org":        "LLDAP",
    "authelia.th3borg.org":     "AUTHELIA",
}

K8S_HOST      = "https://kubernetes.default.svc"
SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_CA_PATH    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

def k8s_token():
    try:
        with open(SA_TOKEN_PATH) as f:
            return f.read().strip()
    except Exception:
        return None

async def get_ingresses(filter_hosts=None) -> list:
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
            services = []
            for item in r.json().get("items", []):
                ns = item["metadata"]["namespace"]
                if ns in SKIP_NAMESPACES:
                    continue
                for rule in item.get("spec", {}).get("rules", []):
                    host = rule.get("host", "")
                    if not host or not host.endswith("th3borg.org"):
                        continue
                    if filter_hosts and host not in filter_hosts:
                        continue
                    paths = rule.get("http", {}).get("paths", [])
                    if not paths:
                        continue
                    backend = paths[0].get("backend", {}).get("service", {})
                    svc_name = backend.get("name", "")
                    svc_port = backend.get("port", {}).get("number", 80)
                    internal = f"http://{svc_name}.{ns}:{svc_port}"
                    display = NAME_OVERRIDES.get(host, host.split(".")[0].upper())
                    services.append({"name": display, "url": f"https://{host}", "internal": internal})
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

async def prom_query(client: httpx.AsyncClient, query: str):
    try:
        r = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=3.0,
        )
        return r.json()["data"]["result"]
    except Exception:
        return []

async def fetch_cluster_stats() -> dict:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            prom_query(client, "100 - (avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100)"),
            prom_query(client, "node_memory_MemTotal_bytes"),
            prom_query(client, "node_memory_MemAvailable_bytes"),
            prom_query(client, "node_filesystem_size_bytes{mountpoint=\"/\"}"),
            prom_query(client, "node_filesystem_avail_bytes{mountpoint=\"/\"}"),
            prom_query(client, "count by (node) (kube_pod_info)"),
            prom_query(client, "node_time_seconds - node_boot_time_seconds"),
            prom_query(client, "kube_node_info"),
        )
    cpu_res, mem_total_res, mem_avail_res, disk_total_res, disk_avail_res, pods_res, uptime_res, node_info_res = results

    instance_to_node = {}
    for r in node_info_res:
        internal_ip = r["metric"].get("internal_ip", "")
        node = r["metric"].get("node", "")
        if internal_ip:
            instance_to_node[internal_ip] = node

    def get_node_name(instance: str) -> str:
        return instance_to_node.get(instance.split(":")[0], instance.split(":")[0])

    def build_node_map(res, transform=float):
        m = {}
        for r in res:
            node = get_node_name(r["metric"].get("instance", ""))
            try:
                m[node] = transform(r["value"][1])
            except Exception:
                pass
        return m

    cpu_map        = build_node_map(cpu_res,        lambda v: round(float(v), 1))
    mem_total_map  = build_node_map(mem_total_res)
    mem_avail_map  = build_node_map(mem_avail_res)
    disk_total_map = build_node_map(disk_total_res)
    disk_avail_map = build_node_map(disk_avail_res)
    uptime_map     = build_node_map(uptime_res,     lambda v: int(float(v)))

    pods_map = {}
    for r in pods_res:
        node = r["metric"].get("node", "")
        try:
            pods_map[node] = int(r["value"][1])
        except Exception:
            pass

    all_nodes = set(list(cpu_map.keys()) + list(mem_total_map.keys()))
    nodes = []
    for node in sorted(all_nodes):
        mt = mem_total_map.get(node)
        ma = mem_avail_map.get(node)
        dt = disk_total_map.get(node)
        da = disk_avail_map.get(node)
        nodes.append({
            "name":           node,
            "cpu_pct":        cpu_map.get(node),
            "mem_total_gb":   round(mt / 1073741824, 1) if mt else None,
            "mem_avail_gb":   round(ma / 1073741824, 1) if ma else None,
            "mem_used_pct":   round(((mt - ma) / mt) * 100, 1) if mt and ma else None,
            "disk_total_gb":  round(dt / 1073741824, 1) if dt else None,
            "disk_avail_gb":  round(da / 1073741824, 1) if da else None,
            "disk_used_pct":  round(((dt - da) / dt) * 100, 1) if dt and da else None,
            "pods":           pods_map.get(node),
            "uptime_seconds": uptime_map.get(node),
        })

    total_mem  = sum(v for v in mem_total_map.values() if v)
    avail_mem  = sum(v for v in mem_avail_map.values() if v)
    total_disk = sum(v for v in disk_total_map.values() if v)
    avail_disk = sum(v for v in disk_avail_map.values() if v)
    total_pods = sum(pods_map.values())
    avg_cpu    = round(sum(cpu_map.values()) / len(cpu_map), 1) if cpu_map else None
    min_uptime = min(uptime_map.values()) if uptime_map else None

    return {
        "nodes": nodes,
        "cluster": {
            "cpu_pct":        avg_cpu,
            "mem_total_gb":   round(total_mem / 1073741824, 1) if total_mem else None,
            "mem_avail_gb":   round(avail_mem / 1073741824, 1) if avail_mem else None,
            "mem_used_pct":   round(((total_mem - avail_mem) / total_mem) * 100, 1) if total_mem and avail_mem else None,
            "disk_total_gb":  round(total_disk / 1073741824, 1) if total_disk else None,
            "disk_avail_gb":  round(avail_disk / 1073741824, 1) if avail_disk else None,
            "disk_used_pct":  round(((total_disk - avail_disk) / total_disk) * 100, 1) if total_disk and avail_disk else None,
            "pods":           total_pods,
            "node_count":     len(all_nodes),
            "uptime_seconds": min_uptime,
        },
    }

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

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/v1/cluster/status")
async def cluster_status(_: str = Depends(require_scope("full"))):
    services = await get_ingresses()
    async with httpx.AsyncClient() as client:
        service_results = await asyncio.gather(*[check_service(client, s) for s in services])
    stats = await fetch_cluster_stats()
    issues = await fetch_glitchtip_issues()
    return {"services": list(service_results), "nodes": stats["nodes"], "cluster": stats["cluster"], "errors": issues}

@app.get("/v1/cluster/nodes")
async def cluster_nodes(_: str = Depends(require_scope("full"))):
    stats = await fetch_cluster_stats()
    return {"nodes": stats["nodes"], "cluster": stats["cluster"]}

@app.get("/v1/cluster/uptime")
async def cluster_uptime(_: str = Depends(require_scope("full", "uptime"))):
    stats = await fetch_cluster_stats()
    return {"uptime_seconds": stats["cluster"]["uptime_seconds"]}

@app.get("/v1/services")
async def services(_: str = Depends(require_scope("full"))):
    svcs = await get_ingresses()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[check_service(client, s) for s in svcs])
    return {"services": list(results)}

@app.get("/v1/services/apps")
async def services_apps(_: str = Depends(require_scope("full", "apps"))):
    svcs = await get_ingresses(filter_hosts=APPS_PORTAL_SERVICES)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[check_service(client, s) for s in svcs])
    return {"services": list(results)}

@app.get("/v1/errors")
async def errors(_: str = Depends(require_scope("full"))):
    return {"errors": await fetch_glitchtip_issues()}
