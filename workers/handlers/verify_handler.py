# handlers/verify_handler.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from kubernetes import client
from kubernetes.client import ApiException

# --------- Spec models ---------

@dataclass
class WorkloadSpec:
    kind: str                         # "deployment" | "statefulset"
    namespace: str
    name_contains: Optional[str] = None
    label_selector: Optional[str] = None  # e.g. "app=kafka,component=broker"
    min_ready_ratio: float = 1.0          # 1.0 = all replicas ready
    require_any: bool = False             # if multiple match: any vs all must be ready

@dataclass
class SecretSpec:
    namespace: str
    names: Optional[List[str]] = None     # exact names that must exist
    prefixes: Optional[List[str]] = None  # any secret starting with one of these

# --------- Helpers ---------

def _ns_exists(core: client.CoreV1Api, ns: str) -> Tuple[bool, Optional[str]]:
    try:
        core.read_namespace(ns)
        return True, None
    except ApiException as e:
        if e.status == 404:
            return False, None
        return False, str(e)

def _filter_by_name(items, contains: Optional[str]):
    if not contains:
        return items
    needle = contains.lower()
    out = []
    for obj in items:
        name = (obj.metadata and obj.metadata.name) or ""
        if needle in name.lower():
            out.append(obj)
    return out

def _deployment_ready(dep: client.V1Deployment, min_ready_ratio: float) -> Dict[str, Any]:
    desired = (dep.spec.replicas or 1) if dep.spec else 1
    ready = dep.status.ready_replicas or 0
    ok = (ready / max(desired, 1)) >= min_ready_ratio and desired > 0
    return {"kind": "deployment", "name": dep.metadata.name, "desired": desired, "ready": ready, "ok": ok}

def _statefulset_ready(sts: client.V1StatefulSet, min_ready_ratio: float) -> Dict[str, Any]:
    desired = (sts.spec.replicas or 1) if sts.spec else 1
    ready = sts.status.ready_replicas or 0
    ok = (ready / max(desired, 1)) >= min_ready_ratio and desired > 0
    return {"kind": "statefulset", "name": sts.metadata.name, "desired": desired, "ready": ready, "ok": ok}

def _list_workloads(apps: client.AppsV1Api, spec: WorkloadSpec):
    if spec.kind.lower() == "deployment":
        resp = apps.list_namespaced_deployment(namespace=spec.namespace, label_selector=spec.label_selector)
        return _filter_by_name(resp.items or [], spec.name_contains)
    elif spec.kind.lower() == "statefulset":
        resp = apps.list_namespaced_stateful_set(namespace=spec.namespace, label_selector=spec.label_selector)
        return _filter_by_name(resp.items or [], spec.name_contains)
    else:
        raise ValueError(f"Unsupported kind: {spec.kind}")

# --------- Public API ---------

def verify_bundle(
    *,
    namespaces: List[str],
    workload_specs: List[WorkloadSpec],
    secret_specs: List[SecretSpec],
) -> Dict[str, Any]:
    """
    Returns:
    {
      "ready": bool,
      "namespaces": [{"name","exists","error"}...],
      "workloads": {"ready": bool, "sections": [
          {"spec": {...}, "ready": bool, "matches": [{"kind","name","desired","ready","ok"}...]}
      ]},
      "secrets": {"ready": bool, "sections": [
          {"namespace","ready", "missing_exact":[...], "missing_prefix":[...], "error": str|None}
      ]},
    }
    """
    core = client.CoreV1Api()
    apps = client.AppsV1Api()

    # Namespaces
    ns_details, ns_ok = [], True
    for ns in namespaces:
        exists, err = _ns_exists(core, ns)
        ns_details.append({"name": ns, "exists": exists, "error": err})
        if not exists or err:
            ns_ok = False

    # Workloads
    wl_sections, wl_ok = [], True
    for spec in workload_specs or []:
        items = _list_workloads(apps, spec)
        matches = []
        for obj in items:
            if spec.kind.lower() == "deployment":
                matches.append(_deployment_ready(obj, spec.min_ready_ratio))
            else:
                matches.append(_statefulset_ready(obj, spec.min_ready_ratio))
        if not items:
            section_ready = False
        elif spec.require_any:
            section_ready = any(m["ok"] for m in matches)
        else:
            section_ready = all(m["ok"] for m in matches)
        wl_sections.append({"spec": spec.__dict__, "ready": section_ready, "matches": matches})
        if not section_ready:
            wl_ok = False

    # Secrets
    sec_sections, sec_ok = [], True
    for ss in secret_specs or []:
        try:
            resp = core.list_namespaced_secret(ss.namespace)
            names = [s.metadata.name for s in (resp.items or []) if s.metadata and s.metadata.name]
            missing_exact, missing_prefix = [], []
            if ss.names:
                for n in ss.names:
                    if n not in names:
                        missing_exact.append(n)
            if ss.prefixes:
                for pref in ss.prefixes:
                    if not any(n.startswith(pref) for n in names):
                        missing_prefix.append(pref)
            section_ok = not missing_exact and not missing_prefix
            sec_sections.append({
                "namespace": ss.namespace, "ready": section_ok,
                "missing_exact": missing_exact, "missing_prefix": missing_prefix, "error": None
            })
            if not section_ok:
                sec_ok = False
        except ApiException as e:
            sec_sections.append({"namespace": ss.namespace, "ready": False,
                                 "missing_exact": [], "missing_prefix": [], "error": str(e)})
            sec_ok = False

    overall = ns_ok and wl_ok and sec_ok
    return {
        "ready": overall,
        "namespaces": ns_details,
        "workloads": {"ready": wl_ok, "sections": wl_sections},
        "secrets": {"ready": sec_ok, "sections": sec_sections},
    }

def summarize_verify_result(res: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Return (ready, reasons) where `reasons` is a concise list of issues.
    Safe no-op if keys are missing.
    """
    reasons: List[str] = []
    ready = bool(res.get("ready", False))

    # Namespaces
    for ns in res.get("namespaces", []):
        if not ns.get("exists", False):
            reasons.append(f"namespace:{ns.get('name')} missing")
        if ns.get("error"):
            reasons.append(f"namespace:{ns.get('name')} error={ns['error']}")

    # Secrets
    for sec in res.get("secrets", {}).get("sections", []):
        if not sec.get("ready", False):
            missing_exact = sec.get("missing_exact") or []
            missing_prefix = sec.get("missing_prefix") or []
            if missing_exact:
                reasons.append(f"secrets ns={sec.get('namespace')} missing={missing_exact}")
            if missing_prefix:
                reasons.append(f"secrets ns={sec.get('namespace')} missing_prefix={missing_prefix}")
            if sec.get("error"):
                reasons.append(f"secrets ns={sec.get('namespace')} error={sec['error']}")

    # Workloads
    for wl in res.get("workloads", {}).get("sections", []):
        if not wl.get("ready", False):
            spec = wl.get("spec", {})
            reasons.append(
                f"workload kind={spec.get('kind')} ns={spec.get('namespace')} "
                f"selector={spec.get('label_selector') or spec.get('name_contains')} not ready"
            )
            for m in wl.get("matches", []):
                if not m.get("ok", False):
                    reasons.append(
                        f"  {m.get('kind')} name={m.get('name')} ready={m.get('ready')}/{m.get('desired')}"
                    )

    return ready, reasons