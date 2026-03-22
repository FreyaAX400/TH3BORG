import asyncio
import httpx
import os
import time
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

app = FastAPI(title="BORGN3T", version="2.0.0", docs_url=None, redoc_url=None)

# ─── Auth ────────────────────────────────────────────────────────────────────

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
        "https://borg.th3borg.org",
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization"],
)

# ─── Config ──────────────────────────────────────────────────────────────────

PROMETHEUS_URL  = os.getenv("PROMETHEUS_URL", "http://kube-prometheus-stack-prometheus.monitoring:9090/prometheus")
GLITCHTIP_URL   = os.getenv("GLITCHTIP_URL",  "http://glitchtip-web.glitchtip:8000")
GLITCHTIP_TOKEN = os.getenv("GLITCHTIP_TOKEN", "")
OLLAMA_URL      = os.getenv("OLLAMA_URL", "http://100.106.53.62:11434")

K8S_HOST      = "https://kubernetes.default.svc"
SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_CA_PATH    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

SKIP_NAMESPACES = {"kube-system", "nginx-ingress", "registry", "homepage", "api"}

ANN_PORTAL      = "th3borg.org/apps-portal"
ANN_NAME        = "th3borg.org/apps-name"
ANN_DESC        = "th3borg.org/apps-description"
ANN_ICON        = "th3borg.org/apps-icon"
ANN_CATEGORY    = "th3borg.org/apps-category"
ANN_TIER        = "th3borg.org/apps-tier"
ANN_DEPENDS     = "th3borg.org/apps-depends"
ANN_DOCS        = "th3borg.org/apps-docs"
ANN_HEALTHCHECK = "th3borg.org/apps-healthcheck"

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
    "borg.th3borg.org":         "BORG",
}

KNOWN_HEALTH_PATHS = {
    "argocd.th3borg.org":       "/healthz",
    "grafana.th3borg.org":      "/api/health",
    "glitchtip.th3borg.org":    "/api/0/",
    "vaultwarden.th3borg.org":  "/api/config",
    "bookstack.th3borg.org":    "/status",
    "uptime.th3borg.org":       "/status",
    "borg.th3borg.org":         "/api/health",
    "search.th3borg.org":       "/healthz",
    "paste.th3borg.org":        "/",
    "speedtest.th3borg.org":    "/api/healthcheck",
    "music.th3borg.org":        "/ping",
}

# ─── Kubernetes ──────────────────────────────────────────────────────────────

def k8s_token():
    try:
        with open(SA_TOKEN_PATH) as f:
            return f.read().strip()
    except Exception:
        return None

async def k8s_get(path: str) -> dict:
    token = k8s_token()
    if not token:
        return {}
    try:
        async with httpx.AsyncClient(verify=SA_CA_PATH) as client:
            r = await client.get(
                f"{K8S_HOST}{path}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5.0,
            )
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        print(f"k8s error {path}: {e}")
    return {}

async def get_pod_readiness() -> dict:
    data = await k8s_get("/api/v1/pods")
    result = {}
    for pod in data.get("items", []):
        ns = pod["metadata"]["namespace"]
        if ns in SKIP_NAMESPACES:
            continue
        labels = pod["metadata"].get("labels", {})
        app = labels.get("app") or labels.get("app.kubernetes.io/name", "")
        if not app:
            continue
        key = f"{ns}/{app}"
        if key not in result:
            result[key] = {"ready": 0, "total": 0, "restarts": 0}
        result[key]["total"] += 1
        restarts = 0
        for cs in pod.get("status", {}).get("containerStatuses", []):
            restarts += cs.get("restartCount", 0)
            if cs.get("ready"):
                result[key]["ready"] += 1
        result[key]["restarts"] += restarts
    return result

async def get_ingresses(apps_portal_only=False) -> list:
    data = await k8s_get("/apis/networking.k8s.io/v1/ingresses")
    services = []
    for item in data.get("items", []):
        ns = item["metadata"]["namespace"]
        annotations = item["metadata"].get("annotations", {})
        if ns in SKIP_NAMESPACES:
            continue
        if apps_portal_only:
            if annotations.get(ANN_PORTAL, "").lower() != "true":
                continue
        for rule in item.get("spec", {}).get("rules", []):
            host = rule.get("host", "")
            if not host or not host.endswith("th3borg.org"):
                continue
            paths = rule.get("http", {}).get("paths", [])
            if not paths:
                continue
            backend = paths[0].get("backend", {}).get("service", {})
            svc_name = backend.get("name", "")
            svc_port = backend.get("port", {}).get("number", 80)
            internal_base = f"http://{svc_name}.{ns}:{svc_port}"
            display = annotations.get(ANN_NAME) or NAME_OVERRIDES.get(host, host.split(".")[0].upper())
            health_path = (
                annotations.get(ANN_HEALTHCHECK)
                or KNOWN_HEALTH_PATHS.get(host)
                or "/"
            )
            services.append({
                "name": display,
                "url": f"https://{host}",
                "internal": internal_base,
                "health_url": f"{internal_base}{health_path}",
                "description": annotations.get(ANN_DESC, ""),
                "icon": annotations.get(ANN_ICON, ""),
                "category": annotations.get(ANN_CATEGORY, ""),
                "tier": annotations.get(ANN_TIER, ""),
                "depends": annotations.get(ANN_DEPENDS, ""),
                "docs": annotations.get(ANN_DOCS, ""),
                "namespace": ns,
                "hostname": host,
            })
    services.sort(key=lambda x: x["name"])
    return services

# ─── Health Checking ─────────────────────────────────────────────────────────

async def check_service(
    client: httpx.AsyncClient,
    svc: dict,
    pod_readiness: dict,
) -> dict:
    ns = svc["namespace"]
    svc_name_lower = svc["name"].lower().replace(" ", "-")
    pod_key = None
    for k in pod_readiness:
        k_ns, k_app = k.split("/", 1)
        if k_ns == ns and (svc_name_lower in k_app or k_app in svc_name_lower):
            pod_key = k
            break

    pod_info = pod_readiness.get(pod_key, {})
    pod_total = pod_info.get("total", 0)
    pod_ready = pod_info.get("ready", 0)
    pod_restarts = pod_info.get("restarts", 0)

    if pod_total > 0 and pod_ready == 0:
        return {
            **svc,
            "status": "down",
            "ping": None,
            "code": None,
            "pods_ready": pod_ready,
            "pods_total": pod_total,
            "restarts": pod_restarts,
            "reason": "no pods ready",
        }

    start = time.monotonic()
    try:
        r = await client.get(svc["health_url"], timeout=8.0, follow_redirects=False)
        ping = int((time.monotonic() - start) * 1000)

        if r.status_code >= 500:
            http_status = "down"
            reason = f"http {r.status_code}"
        elif r.status_code in (401, 403, 302, 301):
            http_status = "up" if ping < 5000 else "degraded"
            reason = "auth-protected"
        elif r.status_code < 400:
            http_status = "up" if ping < 5000 else "degraded"
            reason = "ok"
        else:
            http_status = "degraded"
            reason = f"http {r.status_code}"

        if pod_restarts > 5:
            if http_status == "up":
                http_status = "degraded"
            reason = f"{reason} (restarts: {pod_restarts})"

        if pod_total > 0 and pod_ready < pod_total:
            http_status = "degraded"
            reason = f"{reason} ({pod_ready}/{pod_total} pods ready)"

        return {
            **svc,
            "status": http_status,
            "ping": ping,
            "code": r.status_code,
            "pods_ready": pod_ready,
            "pods_total": pod_total,
            "restarts": pod_restarts,
            "reason": reason,
        }

    except httpx.TimeoutException:
        return {
            **svc,
            "status": "down",
            "ping": None,
            "code": None,
            "pods_ready": pod_ready,
            "pods_total": pod_total,
            "restarts": pod_restarts,
            "reason": "timeout",
        }
    except Exception as e:
        status = "degraded" if pod_ready > 0 else "down"
        return {
            **svc,
            "status": status,
            "ping": None,
            "code": None,
            "pods_ready": pod_ready,
            "pods_total": pod_total,
            "restarts": pod_restarts,
            "reason": str(e),
        }

# ─── Prometheus ──────────────────────────────────────────────────────────────

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
            prom_query(client, '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)'),
            prom_query(client, 'node_memory_MemTotal_bytes'),
            prom_query(client, 'node_memory_MemAvailable_bytes'),
            prom_query(client, 'node_filesystem_size_bytes{mountpoint="/"}'),
            prom_query(client, 'node_filesystem_avail_bytes{mountpoint="/"}'),
            prom_query(client, 'count by (node) (kube_pod_info)'),
            prom_query(client, 'node_time_seconds - node_boot_time_seconds'),
            prom_query(client, 'kube_node_info'),
            prom_query(client, 'node_load1'),
            prom_query(client, 'node_load5'),
        )
    (cpu_res, mem_total_res, mem_avail_res, disk_total_res, disk_avail_res,
     pods_res, uptime_res, node_info_res, load1_res, load5_res) = results

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
    load1_map      = build_node_map(load1_res,      lambda v: round(float(v), 2))
    load5_map      = build_node_map(load5_res,      lambda v: round(float(v), 2))

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
            "name": node,
            "cpu_pct": cpu_map.get(node),
            "load1": load1_map.get(node),
            "load5": load5_map.get(node),
            "mem_total_gb": round(mt / 1073741824, 1) if mt else None,
            "mem_avail_gb": round(ma / 1073741824, 1) if ma else None,
            "mem_used_pct": round(((mt - ma) / mt) * 100, 1) if mt and ma else None,
            "disk_total_gb": round(dt / 1073741824, 1) if dt else None,
            "disk_avail_gb": round(da / 1073741824, 1) if da else None,
            "disk_used_pct": round(((dt - da) / dt) * 100, 1) if dt and da else None,
            "pods": pods_map.get(node),
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
            "cpu_pct": avg_cpu,
            "mem_total_gb": round(total_mem / 1073741824, 1) if total_mem else None,
            "mem_avail_gb": round(avail_mem / 1073741824, 1) if avail_mem else None,
            "mem_used_pct": round(((total_mem - avail_mem) / total_mem) * 100, 1) if total_mem and avail_mem else None,
            "disk_total_gb": round(total_disk / 1073741824, 1) if total_disk else None,
            "disk_avail_gb": round(avail_disk / 1073741824, 1) if avail_disk else None,
            "disk_used_pct": round(((total_disk - avail_disk) / total_disk) * 100, 1) if total_disk and avail_disk else None,
            "pods": total_pods,
            "node_count": len(all_nodes),
            "uptime_seconds": min_uptime,
        },
    }

# ─── External ────────────────────────────────────────────────────────────────

async def fetch_ollama_status() -> dict:
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{OLLAMA_URL}/api/tags", timeout=5.0)
            if r.status_code == 200:
                models = [m["name"] for m in r.json().get("models", [])]
                ps = await client.get(f"{OLLAMA_URL}/api/ps", timeout=5.0)
                running = []
                if ps.status_code == 200:
                    running = [m["name"] for m in ps.json().get("models", [])]
                return {"status": "up", "models": models, "running": running, "model_count": len(models)}
    except Exception as e:
        return {"status": "down", "error": str(e), "models": [], "running": []}

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
                return [{
                    "title": i.get("title") or i.get("culprit", "Unknown"),
                    "project": i.get("project", {}).get("name", "unknown"),
                    "count": i.get("count", 0),
                    "level": i.get("level", "error"),
                    "lastSeen": i.get("lastSeen", ""),
                } for i in r.json()]
    except Exception:
        pass
    return []

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz():
    return {"ok": True, "version": "2.0.0"}

@app.get("/v1/context")
async def context(_: str = Depends(require_scope("full"))):
    pod_readiness, stats, ollama, issues = await asyncio.gather(
        get_pod_readiness(),
        fetch_cluster_stats(),
        fetch_ollama_status(),
        fetch_glitchtip_issues(),
    )
    services_raw = await get_ingresses()
    async with httpx.AsyncClient() as client:
        service_results = await asyncio.gather(
            *[check_service(client, s, pod_readiness) for s in services_raw]
        )
    up   = sum(1 for s in service_results if s["status"] == "up")
    down = sum(1 for s in service_results if s["status"] == "down")
    deg  = sum(1 for s in service_results if s["status"] == "degraded")
    return {
        "timestamp": time.time(),
        "cluster": stats["cluster"],
        "nodes": stats["nodes"],
        "services": {
            "summary": {"up": up, "down": down, "degraded": deg, "total": len(service_results)},
            "list": list(service_results),
        },
        "inference": ollama,
        "errors": issues,
    }

@app.get("/v1/cluster/status")
async def cluster_status(_: str = Depends(require_scope("full"))):
    pod_readiness, stats, issues = await asyncio.gather(
        get_pod_readiness(),
        fetch_cluster_stats(),
        fetch_glitchtip_issues(),
    )
    services_raw = await get_ingresses()
    async with httpx.AsyncClient() as client:
        service_results = await asyncio.gather(
            *[check_service(client, s, pod_readiness) for s in services_raw]
        )
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
    pod_readiness = await get_pod_readiness()
    svcs = await get_ingresses()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[check_service(client, s, pod_readiness) for s in svcs])
    return {"services": list(results)}

@app.get("/v1/services/apps")
async def services_apps(_: str = Depends(require_scope("full", "apps"))):
    pod_readiness = await get_pod_readiness()
    svcs = await get_ingresses(apps_portal_only=True)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[check_service(client, s, pod_readiness) for s in svcs])
    return {"services": list(results)}

@app.get("/v1/services/{hostname:path}")
async def service_detail(hostname: str, _: str = Depends(require_scope("full"))):
    pod_readiness = await get_pod_readiness()
    svcs = await get_ingresses()
    svc = next((s for s in svcs if s["hostname"] == hostname), None)
    if not svc:
        raise HTTPException(status_code=404, detail="Service not found")
    async with httpx.AsyncClient() as client:
        result = await check_service(client, svc, pod_readiness)
    return result

@app.get("/v1/inference")
async def inference(_: str = Depends(require_scope("full"))):
    return await fetch_ollama_status()

@app.get("/v1/errors")
async def errors(_: str = Depends(require_scope("full"))):
    return {"errors": await fetch_glitchtip_issues()}

@app.get("/v1/registry")
async def registry(_: str = Depends(require_scope("full"))):
    svcs = await get_ingresses()
    return {
        "services": svcs,
        "count": len(svcs),
        "categories": list(set(s["category"] for s in svcs if s["category"])),
        "tiers": list(set(s["tier"] for s in svcs if s["tier"])),
    }

@app.get("/v1/annotations")
async def annotations(_: str = Depends(require_scope("full"))):
    return {
        "schema": {
            ANN_PORTAL:      "boolean — include in apps portal",
            ANN_NAME:        "string — display name",
            ANN_DESC:        "string — description",
            ANN_ICON:        "string — icon identifier (reserved)",
            ANN_CATEGORY:    "string — category grouping (media, infra, dev, ai, etc.)",
            ANN_TIER:        "string — tier (core, optional, dev)",
            ANN_DEPENDS:     "string — comma-separated service dependencies",
            ANN_DOCS:        "string — URL to documentation",
            ANN_HEALTHCHECK: "string — health check path override (e.g. /api/health)",
        }
    }
