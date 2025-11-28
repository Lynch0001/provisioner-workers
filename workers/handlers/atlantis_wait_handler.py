# workers/handlers/atlantis_wait_handler.py
import datetime as dt
import random

from gitlab import GitlabHttpError

from workers.config.app_config import ATLANTIS_POLL_SECS, ATLANTIS_MAX_MINUTES, ATLANTIS_USERNAMES

POLL_SECS   = ATLANTIS_POLL_SECS
MAX_MINUTES = ATLANTIS_MAX_MINUTES
ATLANTIS_USERNAMES = ATLANTIS_USERNAMES

TRANSIENT_CODES = {429, 502, 503, 504}

def _sleep_secs():
    # jitter to avoid excessive polling that can trigger cloudflare challenge
    return POLL_SECS + random.randint(0, 10)

def _is_cloudflare_challenge(resp_text: str | None) -> bool:
    if not resp_text:
        return False
    t = resp_text.lower()
    return ("just a moment" in t) or ("__cf_chl" in t) or ("challenge-platform" in t)


def _is_transient_http(err: GitlabHttpError) -> bool:
    code = int(getattr(err, "response_code", 0))
    body = getattr(err, "response_body", "") or ""
    # CF challenge or generic 429/5xx → transient
    if code in (429, 502, 503, 504):
        return True
    if code == 403 and _is_cloudflare_challenge(body):
        return True
    # network timeouts also treated transient by Celery retry earlier
    return False

def _find_latest_atlantis_note(project, mr_iid: int, dir_hint: str | None) -> str | None:
    """
    Returns the raw markdown/body of the most recent Atlantis note for this MR.
    Optionally filter on -d dir (dir_hint).
    """
    notes = project.mergerequests.get(mr_iid).notes.list(
        order_by="created_at", sort="desc", all=True
    )
    for n in notes:
        author = (n.author or {}).get("username", "").lower()
        body   = n.body or ""
        if author in ATLANTIS_USERNAMES in body.lower():
            if dir_hint:
                # only accept notes that mention the dir or include the cmd with -d
                if dir_hint not in body:
                    continue
            return body
    return None

def _pick_atlantis_note_text(notes, dir_hint: str | None) -> str | None:
    for n in notes:  # already newest-first
        author = (n.author or {}).get("username", "").lower()
        body = n.body or ""
        if author in ATLANTIS_USERNAMES in body.lower():
            if not dir_hint or dir_hint in body:
                return body
    return None

def _parse_plan_status(note_body: str) -> str | None:
    if not note_body:
        return None
    text = note_body.lower()
    if "plan succeeded" in text or "plan success" in text:
        return "success"
    if "plan failed" in text or "no changes" in text and "error" not in text:
        # treat 'no changes' as success for plan stage
        return "success"
    if "running" in text or "pending" in text or "in progress" in text:
        return "running"
    if "error" in text or "failed" in text:
        return "failed"
    return None

def _parse_apply_status(note_body: str) -> str | None:
    if not note_body:
        return None
    text = note_body.lower()
    if "apply succeeded" in text or "applied successfully" in text:
        return "success"
    if "apply failed" in text or "error" in text:
        return "failed"
    if "apply pending" in text or "running" in text or "in progress" in text:
        return "running"
    return None

def _deadline(ts_started: dt.datetime) -> dt.datetime:
    return ts_started + dt.timedelta(minutes=MAX_MINUTES)

def _now() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=None)