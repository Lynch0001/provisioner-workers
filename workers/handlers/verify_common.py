# workers/handlers/verify_common.py
import logging
from celery.exceptions import Retry as CeleryRetry
from kubernetes import config as k8s_config
from datetime import datetime

from workers.database.db_wrapper import track_step
from workers.handlers.verify_handler import verify_bundle, summarize_verify_result

logger = logging.getLogger(__name__)

KUBECONFIG_FILE_PATH = "/path/to/kubeconfig"  # or env var

def _load_kubeconfig():
    # Keep it explicit; fail fast with clear message
    k8s_config.load_kube_config(config_file=KUBECONFIG_FILE_PATH)

def _safe_prior(prior):
    # normalize prior and make sure project_id stays a string
    if not isinstance(prior, dict) or "project_id" not in prior:
        return {"project_id": str(prior)}
    return prior

def _log_result(tag: str, res: dict):
    # Compact, structured-ish logs
    ready, reasons = summarize_verify_result(res)
    logger.info("[%s] ready=%s reasons=%s", tag, ready, reasons or [])
    # Keep the detailed lines if you like your existing verbose logs:
    for ns in res.get("namespaces", []):
        logger.info("[%s] ns=%s exists=%s err=%s", tag, ns.get("name"), ns.get("exists"), ns.get("error"))
    for sec in res.get("secrets", {}).get("sections", []):
        logger.info("[%s] secrets ns=%s ready=%s miss_exact=%s miss_prefix=%s err=%s",
                    tag, sec.get("namespace"), sec.get("ready"),
                    sec.get("missing_exact"), sec.get("missing_prefix"), sec.get("error"))
    for section in res.get("workloads", {}).get("sections", []):
        spec = section.get("spec", {})
        logger.info("[%s] workloads kind=%s ns=%s contains=%s selector=%s ready=%s matches=%d",
                    tag, spec.get("kind"), spec.get("namespace"),
                    spec.get("name_contains"), spec.get("label_selector"),
                    section.get("ready"), len(section.get("matches", [])))
        for m in section.get("matches", []):
            logger.info("[%s]   %s name=%s desired=%s ready=%s ok=%s",
                        tag, m.get("kind"), m.get("name"), m.get("desired"), m.get("ready"), m.get("ok"))

@track_step  # your existing decorator that logs start/success/fail
def run_verify_task(self, *, prior: dict,
                    namespaces=None, workloads=None, secrets=None,
                    verify_key: str = "verify_result",
                    retry_seconds: int = 30,
                    hard_fail_on_not_ready: bool = False):
    """
    Generic verify runner used by all verification tasks.

    - Loads kubeconfig
    - Calls verify_bundle(namespaces, workload_specs, secret_specs)
    - Logs summary + details
    - If not ready:
        - If hard_fail_on_not_ready=True => retry
        - Else => return result (caller/chain can decide next)
    """
    prior = _safe_prior(prior)
    project_id = prior["project_id"]
    tag = f"verify:{project_id}"

    try:
        _load_kubeconfig()
    except Exception as e:
        raise self.retry(countdown=retry_seconds, args=[], kwargs={"prior": prior}, exc=RuntimeError(f"Kube config error: {e}"))

    try:
        res = verify_bundle(
            namespaces=namespaces or [],
            workload_specs=workloads or [],
            secret_specs=secrets or [],
        )
        _log_result(tag, res)

        ready, reasons = summarize_verify_result(res)
        payload = {**prior, verify_key: res, "verified_at": datetime.utcnow().isoformat()}

        if ready:
            return payload

        if hard_fail_on_not_ready:
            # Keep retryable; infra is often eventually consistent
            raise self.retry(countdown=retry_seconds, args=[], kwargs={"prior": prior},
                             exc=RuntimeError(f"Verification not ready: {reasons or 'unknown'}"))

        return payload

    except CeleryRetry:
        raise
    except Exception as e:
        raise self.retry(countdown=retry_seconds, args=[], kwargs={"prior": prior},
                         exc=RuntimeError(f"Unexpected verify error: {e}"))