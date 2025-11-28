import logging
import subprocess
from datetime import timedelta
from pathlib import Path

import gitlab
from gitlab import GitlabGetError
from celery import shared_task
from celery.exceptions import Retry as CeleryRetry

from workers.config.logging_config import setup_logging
from workers.database.db_wrapper import track_step
from workers.handlers.atlantis_handler import post_atlantis_apply_comment
from workers.handlers.git_handler import GitRepoSpec, GitHandler, safe_delete_path
from workers.handlers.gitlab_mr_handler import get_gl_project, get_or_create_mr
from workers.handlers.merge_handler import merge_branch

from workers.config.app_config import GITLAB_BASE_URL, GITLAB_TOKEN, GITLAB_INFR_PROJECT_ID, GIT_INFR_BASE_BRANCH, \
    GIT_REBASE_WAIT_SECONDS, GIT_INFR_REPO_URL, INFR_BASE_PATH_DEMO
from workers.handlers.verify_common import run_verify_task

setup_logging()
logger = logging.getLogger(__name__)

GIT_BRANCH_PREFIX = "auto/destroy-core-ns-"

SKIP_VERIFY_TASKS = True
SKIP_DESTROY_TASKS = True

@shared_task(bind=True, name="workers.tasks.destroy.delete_core_ns", queue="core")
@track_step
def delete_core_ns(self, prior:dict, **kwargs):
    # Observe what Celery actually delivered
    logger.debug("[delete_core_ns] args=%r kwargs=%r", self.request.args, self.request.kwargs)
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
            logger.info("[delete_core_ns] created branch %s", branch_name)

            repo_root = Path(gh.repo.working_tree_dir).resolve()

            rel_path = f"{INFR_BASE_PATH_DEMO}/{project_id}/xib-namespace"
            info = safe_delete_path(repo_root, rel_path)
            logger.info("[delete_core_ns] delete info: %s", info)

            # If nothing changed (path not found and no staged changes), short-circuit
            # But still push a branch if you want a "no-op" MR to track the intent — your call.
            # We'll only commit when there are staged changes:
            #   git diff --cached --quiet exits 1 when there are staged changes.
            rc = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=str(repo_root)).returncode
            if rc != 0:
                gh.commit_and_push(f"Delete core_ns for {project_id}")
            else:
                logger.info("[delete_ns] no staged changes; skipping commit/push")

            return {
                "project_id": project_id,
                "workflow_id": prior["workflow_id"],
                "branch_name": branch_name,
                "branch_exists": False,
                "note": f"[delete:core_ns] started with {branch_name}"
            }
    except CeleryRetry:
        raise
    except (OSError, RuntimeError) as e:
        raise self.retry(countdown=60, exc=e)
    except Exception as e:
        raise self.retry(countdown=60, exc=RuntimeError(f"Unexpected error: {e}"))


@shared_task(bind=True, name="workers.tasks.destroy.create_core_ns_mr", queue="core")
@track_step
def create_core_ns_mr(self, prior:dict, **kwargs):
    logger.debug("[delete_core_ns] args=%r kwargs=%r", self.request.args, self.request.kwargs)
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
            title=f"Destroying {source_branch}",
            remove_source_branch=True,
            squash=False,
            labels=["destroy", "core_ns"],
            description=f"Automated MR for `{project_id}`",
        )
        logger.info("core_ns MR ready: %s", getattr(mr, "web_url", "<no-url>"))
        return {**prior, "mr_iid": mr.iid, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        raise
    except Exception as e:
        # Transient network/api issues → retry with backoff
        raise self.retry(countdown=60, args=[prior], exc=e)


@shared_task(bind=True, name="workers.tasks.destroy.destroy_core_ns", max_retries=5,
             retry_delay=timedelta(minutes=2))
@track_step
def destroy_core_ns(self, prior:dict, *, mr_iid: int | None = None):
    logger.debug("[destroy_core_ns] args=%r kwargs=%r", self.request.args, self.request.kwargs)
    project_id = prior.get("project_id")
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
        atlantis_dir = f"{INFR_BASE_PATH_DEMO}/{project_id}/xib-namespace"
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
        return {"project_id": project_id, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        raise
    except Exception as e:
        # Transient network / race → backoff
        raise self.retry(countdown=60, exc=e)


@shared_task(bind=True, name="workers.tasks.destroy.verify_core_ns",
             retry_backoff=3, retry_backoff_max=60,
             retry_jitter=True, retry_kwargs={"max_retries": 6})
@track_step
def verify_core_ns(self, prior: dict):
    if SKIP_VERIFY_TASKS:
        logger.warning("[verify_core_ns] Skipping Verification of core_ns")
        return {**prior}
    if not isinstance(prior, dict) or "project_id" not in prior:
        prior = {"project_id": str(prior)}
    project_id = prior["project_id"]
    namespace = f"{project_id}-xibus-ms"
    return run_verify_task(
        self,
        prior=prior,
        namespaces=[namespace],
        workloads=[],
        secrets=[],
        verify_key="verify_namespace",
        # For namespace, hard fail if not present:
        hard_fail_on_not_ready=True,
        retry_seconds=30,
    )


@shared_task(bind=True, name="workers.tasks.destroy.merge_core_ns_mr",
             retry_backoff=3, retry_backoff_max=60,
             retry_jitter=True, retry_kwargs={"max_retries": 6})
@track_step
def merge_core_ns_mr(self, prior:dict, **kwargs):
    logger.debug("[merge_core_ns] args=%r kwargs=%r", self.request.args, self.request.kwargs)
    project_id = prior.get("project_id")
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
            log_prefix="[merge_core_ns_mr]",
        )
        return {**prior, "merge_results": merge_results, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        # Let Celery handle the scheduled retry
        raise
    except Exception as e:
        # Catch-all: schedule another retry with context
        raise self.retry(countdown=60, args=[prior], exc=RuntimeError(f"Unexpected error: {e}"))