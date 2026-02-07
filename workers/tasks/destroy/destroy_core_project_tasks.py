import datetime
import logging
import subprocess
from datetime import timedelta
from pathlib import Path

import gitlab
from gitlab import GitlabGetError, GitlabHttpError
from celery import shared_task
from celery.exceptions import Retry as CeleryRetry

from workers.config.logging_config import setup_logging
from workers.database.db_wrapper import track_step
from workers.handlers.atlantis_handler import post_atlantis_apply_comment
from workers.handlers.atlantis_wait_handler import _now, _deadline, _find_latest_atlantis_note, _is_transient_http, \
    _parse_plan_status, _parse_apply_status, _sleep_secs, _pick_atlantis_note_text
from workers.handlers.git_handler import GitRepoSpec, GitHandler, safe_delete_path, tombstone_path
from workers.handlers.gitlab_mr_handler import get_gl_project, get_or_create_mr
from workers.handlers.merge_handler import merge_branch

from workers.config.app_config import GITLAB_BASE_URL, GITLAB_TOKEN, GITLAB_INFR_PROJECT_ID, GIT_INFR_BASE_BRANCH, \
    GIT_REBASE_WAIT_SECONDS, GIT_INFR_REPO_URL, INFR_BASE_PATH_DEMO, ATLANTIS_MAX_MINUTES, ATLANTIS_POLL_SECS

setup_logging()
logger = logging.getLogger(__name__)

GIT_BRANCH_PREFIX = "auto/destroy-core-project-"

SKIP_ATLANTIS_TASKS = True
SKIP_DESTROY_TASKS = True

TOMBSTONE_CONTENT = '''terraform {
  source = "../../../modules/null"
}
'''

@shared_task(bind=True, name="workers.tasks.destroy.delete_core_project", queue="core")
@track_step
def delete_core_project(self, prior:dict, **kwargs):
    logger.info("[%s] args=%r kwargs=%r", self.name, self.request.args, self.request.kwargs)
    # normalize: tolerate kwargs/project_id dicts if needed
    project_id = prior["project_id"]
    mr_iid = prior["mr_iid"]
    if project_id is None:
        project_id = kwargs.get("project_id") or (kwargs.get("art") or {}).get("project_id")
    if not project_id or not isinstance(project_id, str):
        raise self.retry(countdown=60, exc=RuntimeError("Missing or invalid project_id"))
    spec = GitRepoSpec(
        repo_url=GIT_INFR_REPO_URL,
        base_branch=GIT_INFR_BASE_BRANCH,
        branch_prefix=GIT_BRANCH_PREFIX,
    )
    gl = gitlab.Gitlab(GITLAB_BASE_URL, private_token=GITLAB_TOKEN, api_version=4, timeout=20)
    gl.session.headers.update(
        {"User-Agent": "provisioner-workers/1.0 (+https://example.com)"})  # TODO update with provisioner host
    project = gl.projects.get(GITLAB_INFR_PROJECT_ID, lazy=True)  # no fetch yet
    mr = project.mergerequests.get(mr_iid)
    branch_name = mr.source_branch
    logger.info("[delete_core_project] branch=%s", branch_name)
    try:
        with GitHandler(spec, project_id) as gh:
            gh.init_repo()
            repo_root = Path(gh.repo.working_tree_dir).resolve()
            logger.info("[delete_core_project] repo_root=%s", repo_root)
            gh.checkout_existing_branch(repo_root, branch_name)
            relative_path = f"{INFR_BASE_PATH_DEMO}/projects/{project_id}"
            logger.info("[delete_core_project] relative_path=%s", relative_path)
            changed = safe_delete_path(repo_root, relative_path)
            logger.info("[delete_core_project] changed=%s", changed)
            if changed:
                gh.commit_and_push(f"Delete core_project for {project_id}")
            else:
                logger.info("[delete_core_project] nothing to delete; no staged changes")

            return {
                "project_id": project_id,
                "workflow_id": prior["workflow_id"],
                "branch_name": branch_name,
                "branch_exists": False,
                "note": f"[delete:core_project] started with {branch_name}"
            }
    except CeleryRetry:
        raise
    except (OSError, RuntimeError) as e:
        raise self.retry(countdown=60, exc=e)
    except Exception as e:
        raise self.retry(countdown=60, exc=RuntimeError(f"Unexpected error: {e}"))



@shared_task(bind=True, name="workers.tasks.destroy.tombstone_core_project", queue="core")
@track_step
def tombstone_core_project(self, prior:dict, **kwargs):
    logger.info("[%s] args=%r kwargs=%r", self.name, self.request.args, self.request.kwargs)
    # normalize: tolerate kwargs/project_id dicts if needed
    project_id = prior["project_id"]
    if project_id is None:
        project_id = kwargs.get("project_id") or (kwargs.get("art") or {}).get("project_id")
    if not project_id or not isinstance(project_id, str):
        raise self.retry(countdown=60, exc=RuntimeError("Missing or invalid project_id"))
    spec = GitRepoSpec(
        repo_url=GIT_INFR_REPO_URL,
        base_branch=GIT_INFR_BASE_BRANCH,
        branch_prefix=GIT_BRANCH_PREFIX,
    )
    try:
        with GitHandler(spec, project_id) as gh:
            gh.init_repo()
            branch_name_template = f"{GIT_BRANCH_PREFIX}{project_id}"
            branch_name = gh.create_branch(explicit_name=branch_name_template)
            logger.info("[delete_core_project] created branch %s", branch_name)
            repo_root = Path(gh.repo.working_tree_dir).resolve()
            # project terragrunt.hcl
            relative_path = f"{INFR_BASE_PATH_DEMO}/projects/{project_id}/terragrunt.hcl"
            changed = tombstone_path(repo_root, relative_path)
            logger.info("[tombstone_core_project] changed=%s path=%s", changed, relative_path)
            subprocess.run(["git", "add", relative_path], cwd=str(repo_root), check=True)
            rc = subprocess.run(["git", "diff", "--quiet"], cwd=str(repo_root)).returncode
            if rc != 0:
                gh.commit_and_push(f"Tombstone core_project for {project_id}")
            else:
                logger.info("[tombstone_core_project] no staged changes; skipping commit/push")
            return {
                "project_id": project_id,
                "workflow_id": prior["workflow_id"],
                "branch_name": branch_name,
                "branch_exists": False,
                "note": f"[delete:core_project] started with {branch_name}"
            }
    except CeleryRetry:
        raise
    except (OSError, RuntimeError) as e:
        raise self.retry(countdown=60, exc=e)
    except Exception as e:
        raise self.retry(countdown=60, exc=RuntimeError(f"Unexpected error: {e}"))




@shared_task(bind=True, name="workers.tasks.destroy.create_core_project_mr", queue="core_project")
@track_step
def create_core_project_mr(self, prior:dict, **kwargs):
    logger.info("[%s] args=%r kwargs=%r", self.name, self.request.args, self.request.kwargs)
    if not isinstance(prior, dict):
        raise ValueError(f"{self.name} expected dict prior, got {type(prior).__name__}: {prior!r}")
    project_id = prior.get("project_id")
    if not project_id or not isinstance(project_id, str):
        raise self.retry(countdown=60, args=[prior], exc=RuntimeError("Missing or invalid project_id"))
    try:
        project = get_gl_project(GITLAB_BASE_URL, GITLAB_TOKEN, GITLAB_INFR_PROJECT_ID)
        source_branch = f"{GIT_BRANCH_PREFIX}{project_id}"
        # ensure the branch exists remotely (handles race after push)
        try:
            project.branches.get(source_branch)
        except GitlabGetError as e:
            if getattr(e, "response_code", None) == 404:
                # branch not visible yet → retry
                raise self.retry(exc=RuntimeError(f"Branch {source_branch} not found yet"), countdown=30, args=[prior])
            raise  # other errors bubble
        mr = get_or_create_mr(
            project=project,
            source_branch=source_branch,
            target_branch=GIT_INFR_BASE_BRANCH,
            title=f"Provisioning {source_branch}",
            remove_source_branch=True,
            squash=False,
            labels=["provision", "core_project"],
            description=f"Automated MR for `{project_id}`",
        )
        now     = datetime.datetime.now(datetime.timezone.utc)
        logger.info("Core_Project MR ready: %s", getattr(mr, "web_url", "<no-url>"))
        return {**prior, "mr_iid": mr.iid, "workflow_id": prior["workflow_id"], "started_at_iso": now.isoformat()}
    except CeleryRetry:
        raise
    except Exception as e:
        # Transient network/api issues → retry with backoff
        raise self.retry(countdown=60, args=[prior], exc=e)


@shared_task(bind=True, name="workers.tasks.destroy.core_project_wait_atlantis_plan",
             queue="core", max_retries=120, default_retry_delay=10)
@track_step
def core_project_wait_atlantis_plan(self, prior: dict, **kwargs):
    project_id      = prior["project_id"]
    mr_iid          = prior["mr_iid"]
    # TODO Temp
    if SKIP_ATLANTIS_TASKS:
        return {**prior, "project_id": project_id, "mr_iid": mr_iid}
    dir_hint        = prior.get("atlantis_dir")
    started_at      = prior.get("plan_started_at") or prior.get("started_at") or _now()
    if isinstance(started_at, str):
        started_at = datetime.datetime.fromisoformat(started_at)

    if _now() > _deadline(started_at):
        raise RuntimeError(f"[wait_atlantis_plan] timeout after {ATLANTIS_MAX_MINUTES}m for MR !{mr_iid} (dir={dir_hint})")

    try:
        gl = gitlab.Gitlab(GITLAB_BASE_URL, private_token=GITLAB_TOKEN, api_version=4, timeout=20)
        gl.session.headers.update({"User-Agent": "provisioner-workers/1.0 (+https://example.com)"}) # TODO update with provisioner host
        project = gl.projects.get(GITLAB_INFR_PROJECT_ID, lazy=True)  # no fetch yet
        notes = project.mergerequests.get(mr_iid).notes.list(
            order_by="created_at", sort="desc", per_page=50
        )
    except GitlabHttpError as e:
        if _is_transient_http(e):
            raise self.retry(countdown=_sleep_secs(), exc=e)
        raise
    except Exception as e:
        # socket timeouts etc. → transient
        raise self.retry(countdown=_sleep_secs(), exc=e)

    status = _parse_plan_status(_pick_atlantis_note_text(notes, dir_hint))
    if status in (None, "running"):
        raise self.retry(countdown=_sleep_secs())
    if status == "failed":
        raise RuntimeError(f"Atlantis plan failed for !{mr_iid} dir={dir_hint}")
    return {**prior, "project_id": project_id, "mr_iid": mr_iid, "workflow_id": prior["workflow_id"]}


@shared_task(bind=True, name="workers.tasks.destroy.destroy_core_project", max_retries=5,
             retry_delay=timedelta(minutes=2))
@track_step
def destroy_core_project(self, prior:dict, *, mr_iid: int | None = None):
    logger.debug("[destroy_core_project] args=%r kwargs=%r", self.request.args, self.request.kwargs)
    project_id = prior.get("project_id")
    workflow_id = prior.get("workflow_id")
    # If chained, `project_id` might actually be the dict returned by the previous task.
    if isinstance(project_id, dict):
        payload = project_id
        project_id = payload.get("project_id")
        mr_iid = payload.get("mr_iid", mr_iid)
    if not project_id or not isinstance(project_id, str):
        raise self.retry(countdown=60, exc=RuntimeError("Missing or invalid project_id"))
    try:
        project = get_gl_project(GITLAB_BASE_URL, GITLAB_TOKEN, GITLAB_INFR_PROJECT_ID)
        # If mr_iid is not provided, we’ll look up by branches
        source_branch = f"{GIT_BRANCH_PREFIX}{project_id}"
        atlantis_dir = f"{INFR_BASE_PATH_DEMO}/project/{project_id}"
        result = post_atlantis_apply_comment(
            project=project,
            mr_iid=mr_iid,
            source_branch=None if mr_iid is not None else source_branch,
            target_branch=None if mr_iid is not None else GIT_INFR_BASE_BRANCH,
            directory=atlantis_dir,
            terraform_destroy=True
        )
        logger.info("Atlantis apply comment posted: MR !%s %s (note %s)",
                 result["mr_iid"], result["mr_url"], result["note_id"])
        return {"project_id": project_id, "workflow_id": workflow_id, "started_at_iso": prior.get("mr_started"), "mr_iid": mr_iid}
    except CeleryRetry:
        raise
    except Exception as e:
        # Transient network / race → backoff
        raise self.retry(countdown=60, exc=e)


@shared_task(bind=True, name="workers.tasks.destroy.core_project_wait_atlantis_apply",
             queue="core", max_retries=180, default_retry_delay=10)
@track_step
def core_project_wait_atlantis_apply(self, prior: dict, **kwargs):

    """
    Waits for Atlantis Apply to finish. Same expectations as plan wait, but
    looks for apply status in MR notes.
    """
    project_id      = prior["project_id"]
    workflow_id = prior["workflow_id"]
    # TODO Temp
    if SKIP_ATLANTIS_TASKS:
        return {**prior, "project_id": project_id}
    mr_iid          = prior["mr_iid"]
    dir_hint        = prior.get("atlantis_dir")
    started_at      = prior.get("apply_started_at") or prior.get("started_at") or _now()
    if isinstance(started_at, str):
        started_at = datetime.datetime.fromisoformat(started_at)

    if _now() > _deadline(started_at):
        raise RuntimeError(f"[wait_atlantis_apply] timeout after {ATLANTIS_MAX_MINUTES}m for MR !{mr_iid} (dir={dir_hint})")

    try:
        gl = gitlab.Gitlab(GIT_INFR_REPO_URL, private_token=GITLAB_TOKEN)
        project = gl.projects.get(GITLAB_INFR_PROJECT_ID)
        note = _find_latest_atlantis_note(project, mr_iid, dir_hint)
    except gitlab.exceptions.GitlabHttpError as e:
        if _is_transient_http(e):
            raise self.retry(countdown=ATLANTIS_POLL_SECS, exc=e)
        raise

    status = _parse_apply_status(note or "")
    if status in (None, "running"):
        raise self.retry(countdown=ATLANTIS_POLL_SECS)
    if status == "failed":
        raise RuntimeError(f"[wait_atlantis_apply] apply failed for MR !{mr_iid} (dir={dir_hint})")
    prior["apply_waited_at"] = _now().isoformat()
    return {**prior, "project_id": project_id, "workflow_id": workflow_id}

@shared_task(bind=True, name="workers.tasks.destroy.verify_core_project",
             retry_backoff=3, retry_backoff_max=60,
             retry_jitter=True, retry_kwargs={"max_retries": 6})
@track_step
def verify_core_project(self, prior: dict):
    # TODO is there a way to verify project creation
    logger.debug("[verify_core_project] args=%r kwargs=%r", self.request.args, self.request.kwargs)

    if not isinstance(prior, dict) or "project_id" not in prior:
        prior = {"project_id": str(prior)}
    project_id = prior["project_id"]
    logger.warning(f"core_Project not verified")
    return {"project_id": project_id, "workflow_id": prior["workflow_id"]}


@shared_task(bind=True, name="workers.tasks.destroy.merge_core_project_mr",
             retry_backoff=3, retry_backoff_max=60,
             retry_jitter=True, retry_kwargs={"max_retries": 6})
@track_step
def merge_core_project_mr(self, prior:dict, **kwargs):
    logger.info("[%s] args=%r kwargs=%r", self.name, self.request.args, self.request.kwargs)

    project_id = prior.get("project_id")
    workflow_id = prior.get("workflow_id")
    gl = gitlab.Gitlab(GITLAB_BASE_URL, private_token=GITLAB_TOKEN)
    project = gl.projects.get(GITLAB_INFR_PROJECT_ID)
    source_branch = f"{GIT_BRANCH_PREFIX}{project_id}"
    try:
        merge_results = merge_branch(
            retry=self.retry,
            project=project,
            source_branch=source_branch,
            target_branch=GIT_INFR_BASE_BRANCH,
            rebase_wait_seconds=GIT_REBASE_WAIT_SECONDS,
            log_prefix="[merge_core_project_mr]",
        )
        return {**prior, "merge_results": merge_results, "workflow_id": workflow_id}
    except CeleryRetry:
        # Let Celery handle the scheduled retry
        raise
    except Exception as e:
        # Catch-all: schedule another retry with context
        raise self.retry(countdown=60, args=[prior], exc=RuntimeError(f"Unexpected error: {e}"))







    #OLD WAIT_PLAN

    # mr_iid = prior.get("mr_iid")
    # project_id = prior.get("project_id")
    # started_at_iso = prior.get("started_at_iso")
    # atlantis_dir = f"{INFR_BASE_PATH_DEMO}/project/{project_id}"
    # timeout_s = 1800
    # gl = gitlab.Gitlab(GITLAB_BASE_URL, private_token=GITLAB_TOKEN)
    # proj = gl.projects.get(GITLAB_INFR_PROJECT_ID)
    # mr   = proj.mergerequests.get(mr_iid)
    #
    # since   = _iso_to_utc(started_at_iso)
    # now     = datetime.datetime.now(datetime.timezone.utc)
    # elapsed = (now - since).total_seconds()
    # if elapsed > timeout_s:
    #     raise RuntimeError(f"Timed out waiting for Atlantis plan after {timeout_s}s (dir={atlantis_dir}).")
    # min_command_note_id = None # temp evaluate later
    # for note in _mr_notes_since(mr, since, min_note_id=min_command_note_id):
    #     body = note.body or ""
    #     # must be for our dir
    #     if PLAN_HEADER_RE.search(body) and atlantis_dir in body:
    #         outcome = _plan_outcome(body)
    #         if outcome in ("planned", "noop"):
    #             logger.info("Atlantis plan OK for %s on MR !%s", atlantis_dir, mr_iid)
    #             return {**prior, "mr_iid": mr_iid, "plan_status": outcome, "plan_note_id": note.id, "mr_started": started_at_iso}
    #         if outcome == "failed":
    #             raise RuntimeError(f"Atlantis plan FAILED for {atlantis_dir} (MR !{mr_iid}).")
    # raise self.retry()





    #OLD WAIT_APPLY

    # mr_iid = prior.get("mr_iid")
    # project_id = prior.get("project_id")
    # started_at_iso = prior.get("mr_started")
    # atlantis_dir = f"{INFR_BASE_PATH_DEMO}/project/{project_id}"
    # timeout_s = 2700
    # gl = gitlab.Gitlab(GITLAB_BASE_URL, private_token=GITLAB_TOKEN)
    # proj = gl.projects.get(GITLAB_INFR_PROJECT_ID)
    # mr   = proj.mergerequests.get(mr_iid)
    # since   = _iso_to_utc(started_at_iso)
    # now     = datetime.datetime.now(datetime.timezone.utc)
    # elapsed = (now - since).total_seconds()
    # if elapsed > timeout_s:
    #     raise RuntimeError(f"Timed out waiting for Atlantis apply after {timeout_s}s (dir={atlantis_dir}).")
    # min_command_note_id = None # temp evaluate later
    # for note in _mr_notes_since(mr, since, min_note_id=min_command_note_id):
    #     body = note.body or ""
    #     if APPLY_HEADER_RE.search(body) and atlantis_dir in body:
    #         outcome = _apply_outcome(body)
    #         if outcome == "applied":
    #             logger.info("Atlantis apply OK for %s on MR !%s", atlantis_dir, mr_iid)
    #             return {**prior, "mr_iid": mr_iid, "apply_status": "succeeded", "apply_note_id": note.id}
    #         if outcome in ("failed", "no_plan"):
    #             raise RuntimeError(f"Atlantis apply {outcome} for {atlantis_dir} (MR !{mr_iid}).")
    # raise self.retry()