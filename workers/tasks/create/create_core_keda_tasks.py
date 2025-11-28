import logging
from datetime import timedelta

import gitlab
from gitlab import GitlabGetError
from celery import shared_task
from celery.exceptions import Retry as CeleryRetry

from workers.config.logging_config import setup_logging
from workers.database.db_wrapper import track_step
from workers.handlers.atlantis_handler import post_atlantis_apply_comment
from workers.handlers.git_handler import GitRepoSpec, GitHandler
from workers.handlers.gitlab_mr_handler import get_gl_project, get_or_create_mr
from workers.handlers.merge_handler import merge_branch

from workers.config.app_config import GITLAB_BASE_URL, GITLAB_TOKEN, GITLAB_INFR_PROJECT_ID, GIT_INFR_BASE_BRANCH, \
    GIT_REBASE_WAIT_SECONDS, GIT_INFR_REPO_URL, INFR_BASE_PATH_DEMO
from workers.handlers.verify_common import run_verify_task
from workers.handlers.verify_handler import SecretSpec

setup_logging()
logger = logging.getLogger(__name__)

GIT_BRANCH_PREFIX = "auto/create-core-keda-"

SKIP_VERIFY_TASKS = True

@shared_task(bind=True, name="workers.tasks.create.create_core_keda", queue="core_keda")
@track_step
def create_core_keda(self, prior:dict, **kwargs):
    # Observe what Celery actually delivered
    logger.debug("[create_core_keda] args=%r kwargs=%r", self.request.args, self.request.kwargs)
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
            logger.info("[create_core_keda] created branch %s", branch_name)
            src = f"{INFR_BASE_PATH_DEMO}/alpha/keda-artemis-secret"
            dst = f"{INFR_BASE_PATH_DEMO}/{project_id}/keda-artemis-secret"
            gh.copy_tree(src, dst, dirs_exist_ok=True)
            gh.replace_tokens_in_file(f"{dst}/terragrunt.hcl", {"alpha": project_id})
            if gh.remote_branch_exists(branch_name):
                result = {
                    "project_id": project_id,
                    "branch_name": branch_name,
                    "branch_exists": True,
                    "note": f"[create:core_keda] Branch {branch_name} already exists; skipping push."
                }
                return result
            gh.commit_and_push(f"provisioner core_keda {project_id}")
            return {
                "project_id": project_id,
                "workflow_id": prior["workflow_id"],
                "branch_name": branch_name,
                "branch_exists": False,
                "note": f"[create:core_keda] started with {branch_name}"
            }
    except CeleryRetry:
        raise
    except (OSError, RuntimeError) as e:
        raise self.retry(countdown=60, exc=e)
    except Exception as e:
        raise self.retry(countdown=60, exc=RuntimeError(f"Unexpected error: {e}"))


@shared_task(bind=True, name="workers.tasks.create.create_core_keda_mr", queue="core_keda")
@track_step
def create_core_keda_mr(self, prior:dict, **kwargs):
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
            title=f"Creating {source_branch}",
            remove_source_branch=True,
            squash=False,
            labels=["create", "core_keda"],
            description=f"Automated MR for `{project_id}`",
        )
        logger.info("Core_Keda MR ready: %s", getattr(mr, "web_url", "<no-url>"))
        return {**prior, "mr_iid": mr.iid, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        raise
    except Exception as e:
        # Transient network/api issues → retry with backoff
        raise self.retry(countdown=60, args=[prior], exc=e)


@shared_task(bind=True, name="workers.tasks.create.deploy_core_keda", max_retries=5,
             retry_delay=timedelta(minutes=2))
@track_step
def deploy_core_keda(self, prior:dict, *, mr_iid: int | None = None):
    logger.debug("[create_core_project] args=%r kwargs=%r", self.request.args, self.request.kwargs)
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
        atlantis_dir = f"{INFR_BASE_PATH_DEMO}/{project_id}/keda-artemis-secret"
        result = post_atlantis_apply_comment(
            project=project,
            mr_iid=mr_iid,
            source_branch=None if mr_iid is not None else source_branch,
            target_branch=None if mr_iid is not None else GIT_INFR_BASE_BRANCH,
            directory=atlantis_dir,
        )
        logger.info("Atlantis apply comment posted: MR !%s %s (note %s)",
                 result["mr_iid"], result["mr_url"], result["note_id"])
        return {"project_id": project_id, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        raise
    except Exception as e:
        # Transient network / race → backoff
        raise self.retry(countdown=60, exc=e)


@shared_task(bind=True, name="workers.tasks.create.verify_core_keda",
             retry_backoff=3, retry_backoff_max=60,
             retry_jitter=True, retry_kwargs={"max_retries": 6})
@track_step
def verify_core_keda(self, prior: dict):
    if SKIP_VERIFY_TASKS:
        logger.warning("[verify_core_keda] Skipping Verification of core_keda")
        return {**prior}
    if not isinstance(prior, dict) or "project_id" not in prior:
        prior = {"project_id": str(prior)}
    project_id = prior["project_id"]
    namespace = f"{project_id}-xibus-ms"
    secret_specs = [
        # SecretSpec(namespace=ns, names=["my-critical-secret", "another-one"]),
        # SecretSpec(namespace=namespace, prefixes=["tls-", "app-"]),
        SecretSpec(namespace=namespace, names=["xibus-artemis-keda-secret"]),
    ]
    return run_verify_task(
        self,
        prior=prior,
        namespaces=[namespace],
        workloads=[],
        secrets=[secret_specs],
        verify_key="verify_secrets",
        # For namespace, hard fail if not present:
        hard_fail_on_not_ready=True,
        retry_seconds=45,
    )

@shared_task(bind=True, name="workers.tasks.create.merge_core_keda_mr",
             retry_backoff=3, retry_backoff_max=60,
             retry_jitter=True, retry_kwargs={"max_retries": 6})
@track_step
def merge_core_keda_mr(self, prior:dict, **kwargs):
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
            log_prefix="[merge_core_keda_mr]",
        )
        return {**prior, "merge_results": merge_results, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        # Let Celery handle the scheduled retry
        raise
    except Exception as e:
        # Catch-all: schedule another retry with context
        raise self.retry(countdown=60, args=[prior], exc=RuntimeError(f"Unexpected error: {e}"))