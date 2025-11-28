import time
import gitlab
from gitlab.exceptions import GitlabHttpError, GitlabMRClosedError
from typing import Optional, Tuple, Callable

# You can import these from your settings module
# GIT_REBASE_WAIT_SECONDS = 2

def _reload_mr(project, mr):
    return project.mergerequests.get(mr.iid)

def _pipeline_status(mr) -> Optional[str]:
    hp = getattr(mr, "head_pipeline", None)
    return (hp or {}).get("status") if hp else None

def _requires_green_ci(project, mr) -> bool:
    return bool(getattr(project, "only_allow_merge_if_pipeline_succeeds", False)) or \
           getattr(mr, "detailed_merge_status", "") in ("ci_must_pass", "ci_still_running")

def _get_mr_any_state(project, source_branch: str, target_branch: str) -> Tuple[Optional[object], Optional[str]]:
    """Return (mr, 'opened'|'merged'|'closed') or (None, None)."""
    for state in ("opened", "merged", "closed"):
        mrs = project.mergerequests.list(
            source_branch=source_branch,
            target_branch=target_branch,
            state=state,
            all=True,
            per_page=1,
            order_by="updated_at",
            sort="desc",
        )
        if mrs:
            return project.mergerequests.get(mrs[0].iid), state
    return None, None

def _branch_exists(project, branch_name: str) -> bool:
    try:
        project.branches.get(branch_name)
        return True
    except gitlab.exceptions.GitlabGetError as e:
        if getattr(e, "response_code", None) == 404:
            return False
        raise

def _find_open_mr_with_backoff(project, source_branch: str, target_branch: str, burst_seconds: int = 20):
    """Brief poll to smooth creation races, then return an OPEN MR or None."""
    deadline = time.monotonic() + burst_seconds
    while time.monotonic() < deadline:
        mrs = project.mergerequests.list(
            source_branch=source_branch,
            target_branch=target_branch,
            state="opened",
            all=True,
            per_page=1,
            order_by="updated_at",
            sort="desc",
        )
        if mrs:
            return project.mergerequests.get(mrs[0].iid)
        time.sleep(2)
    return None

def merge_branch(
    *,
    retry: Callable[..., None],              # pass `self.retry` here
    project,                                 # gitlab project object
    source_branch: str,
    target_branch: str,
    rebase_wait_seconds: int = 2,
    log_prefix: str = "[merge_handler]",
) -> str:
    """
    Merge source_branch -> target_branch with idempotency and all GitLab gates.
    Returns success string on completion; otherwise calls `retry(...)` (which raises).
    """

    # 1) Try to locate an MR (any state) quickly; if none, brief open-MR poll; then idempotent checks
    mr, mr_state = _get_mr_any_state(project, source_branch, target_branch)
    if mr is None:
        mr = _find_open_mr_with_backoff(project, source_branch, target_branch, burst_seconds=20)
        if mr is None:
            # If branch was removed, assume already merged/cleaned up
            if not _branch_exists(project, source_branch):
                return f"{log_prefix} source {source_branch} not found; assuming already merged/removed."
            # Otherwise, MR not created yet → retry
            retry(countdown=60, exc=RuntimeError(f"No MR yet for {source_branch} -> {target_branch}"))
            return ""  # not reached

    # If we got MR via any-state path:
    if mr_state == "merged":
        return f"{log_prefix} MR !{mr.iid} already merged: {mr.web_url}"
    if mr_state == "closed":
        raise RuntimeError(f"{log_prefix} MR !{mr.iid} is closed: {mr.web_url}")

    # From here: MR is OPEN
    print(f"{log_prefix} Found OPEN MR !{mr.iid}: {mr.title} {mr.web_url}")

    # 2) Hard gates → retry
    if getattr(mr, "draft", False) or getattr(mr, "work_in_progress", False):
        retry(countdown=60, exc=RuntimeError("MR is Draft/WIP"))
        return ""
    if getattr(mr, "blocking_discussions_resolved", True) is False:
        retry(countdown=60, exc=RuntimeError("Blocking discussions not resolved"))
        return ""

    # Approvals (ignore if API disabled)
    try:
        approvals = mr.approvals.get()
        if hasattr(approvals, "approved") and not approvals.approved:
            retry(countdown=60, exc=RuntimeError("Required approvals missing"))
            return ""
    except Exception:
        pass

    # 3) Conflicts / outdated SHA → try rebase then retry
    if getattr(mr, "has_conflicts", False):
        try:
            mr.rebase()
            time.sleep(rebase_wait_seconds)
        except GitlabHttpError as e:
            retry(countdown=60, exc=RuntimeError(f"Rebase failed: {e}"))
            return ""
        retry(countdown=90, exc=RuntimeError("Rebased; will retry merge"))
        return ""

    # 4) Ensure CI exists if required, then choose MWPS based on pipeline state
    mr = _reload_mr(project, mr)
    head_pipeline_status = _pipeline_status(mr)
    if head_pipeline_status is None and _requires_green_ci(project, mr):
        try:
            project.pipelines.create({"ref": mr.source_branch})
        except GitlabHttpError as e:
            retry(countdown=60, exc=RuntimeError(f"Could not start pipeline: {e}"))
            return ""
        retry(countdown=90, exc=RuntimeError("Started pipeline; waiting for CI"))
        return ""

    mwps = head_pipeline_status in ("running", "pending")

    # 5) Merge attempt (re-get right before merge; optimistic lock with sha)
    mr = _reload_mr(project, mr)
    try:
        mr.merge(
            sha=mr.sha,  # only merge if HEAD unchanged
            squash=True,
            should_remove_source_branch=True,
            merge_when_pipeline_succeeds=mwps,
            merge_commit_message="Merging by Demo Provisioner",
        )
    except GitlabMRClosedError:
        # python-gitlab maps 405 to this sometimes; re-GET to see actual state
        mr = _reload_mr(project, mr)
        if getattr(mr, "state", None) == "merged" or getattr(mr, "merged_at", None):
            return f"{log_prefix} MR !{mr.iid} already merged: {mr.web_url}"
        if getattr(mr, "state", None) == "closed":
            raise RuntimeError(f"{log_prefix} MR !{mr.iid} is closed: {mr.web_url}")
        retry(countdown=60, exc=RuntimeError(
            f"405 not mergeable yet; detailed={getattr(mr,'detailed_merge_status',None)} "
            f"pipeline={_pipeline_status(mr)}"
        ))
        return ""
    except GitlabHttpError as e:
        code = getattr(e, "response_code", None)
        mr = _reload_mr(project, mr)
        detailed = getattr(mr, "detailed_merge_status", None)
        if code == 405:
            retry(countdown=60, exc=RuntimeError(
                f"405 not mergeable yet; detailed={detailed} pipeline={_pipeline_status(mr)}"
            ))
            return ""
        if code in (409, 422):
            retry(countdown=90, exc=RuntimeError(f"{code} transient merge blocker: {e}"))
            return ""
        raise

    # 6) Confirm or MWPS queue
    mr = _reload_mr(project, mr)
    if getattr(mr, "state", None) == "merged" or getattr(mr, "merged_at", None):
        return f"{log_prefix} MR !{mr.iid} merged: {mr.web_url}"

    retry(countdown=60, exc=RuntimeError(
        f"MR !{mr.iid} queued; pipeline={_pipeline_status(mr)}"
    ))
    return ""