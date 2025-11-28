import logging

import gitlab
from gitlab import GitlabGetError
from celery import shared_task
from celery.exceptions import Retry as CeleryRetry

from workers.config.logging_config import setup_logging
from workers.database.db_wrapper import track_step
from workers.handlers.git_handler import GitRepoSpec, GitHandler
from workers.handlers.gitlab_mr_handler import get_gl_project, get_or_create_mr
from workers.handlers.merge_handler import merge_branch

from workers.config.app_config import GITLAB_BASE_URL, GITLAB_TOKEN, GITLAB_HELM_PROJECT_ID, GIT_HELM_BASE_BRANCH, \
    GIT_REBASE_WAIT_SECONDS, GIT_INFR_HELM_URL, ACTIVE_MICROSERVICE_LIST

setup_logging()
logger = logging.getLogger(__name__)

GIT_BRANCH_PREFIX = "auto/create-helm-"


@shared_task(bind=True, name="workers.tasks.create.create_helm_values", queue="helm")
@track_step
def create_helm(self, prior:dict, **kwargs):
    # Observe what Celery actually delivered
    logger.debug("[create_helm] args=%r kwargs=%r", self.request.args, self.request.kwargs)
    # normalize: tolerate kwargs/project_id dicts if needed
    project_id = prior["project_id"]
    if project_id is None:
        project_id = kwargs.get("project_id") or (kwargs.get("art") or {}).get("project_id")
    if not project_id or not isinstance(project_id, str):
        raise self.retry(countdown=60, exc=RuntimeError("Missing or invalid project_id"))
    spec = GitRepoSpec(
        repo_url=GIT_INFR_HELM_URL,
        base_branch=GIT_HELM_BASE_BRANCH,
        branch_prefix=GIT_BRANCH_PREFIX,
    )
    try:
        with GitHandler(spec, project_id) as gh:
            gh.init_repo()
            branch_name_template = f"{GIT_BRANCH_PREFIX}{project_id}"
            branch_name = gh.create_branch(explicit_name=branch_name_template)
            logger.info("[create_helm] created branch %s", branch_name)
            # move to configuration later
            services = ACTIVE_MICROSERVICE_LIST
            for service in services:
                src = f"{service}/values/demo/alpha"
                dst = f"{service}/values/demo/{project_id}"
                gh.copy_tree(src, dst, dirs_exist_ok=True)
                gh.replace_tokens_in_tree(
                    dst,
                    {"alpha": project_id},
                    file_globs=("*.yaml", "*.yml"),
                )

            # TODO sftp user creation

            if gh.remote_branch_exists(branch_name):
                result = {
                    "project_id": project_id,
                    "workflow_id": prior["workflow_id"],
                    "branch_name": branch_name,
                    "branch_exists": True,
                    "note": f"[create:helm] Branch {branch_name} already exists; skipping push."
                }
                return result
            gh.commit_and_push(f"provisioner helm {project_id}")
            return {
                "project_id": project_id,
                "workflow_id": prior["workflow_id"],
                "branch_name": branch_name,
                "branch_exists": False,
                "note": f"[create:helm] started with {branch_name}"
            }
    except CeleryRetry:
        raise
    except (OSError, RuntimeError) as e:
        raise self.retry(countdown=60, exc=e)
    except Exception as e:
        raise self.retry(countdown=60, exc=RuntimeError(f"Unexpected error: {e}"))


@shared_task(bind=True, name="workers.tasks.create.create_helm_values_mr", queue="helm")
@track_step
def create_helm_mr(self, prior:dict, **kwargs):
    project_id = prior.get("project_id")
    if not project_id or not isinstance(project_id, str):
        raise self.retry(countdown=60, args=[prior], exc=RuntimeError("Missing or invalid project_id"))
    try:
        project = get_gl_project(GITLAB_BASE_URL, GITLAB_TOKEN, GITLAB_HELM_PROJECT_ID)
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
            target_branch=GIT_HELM_BASE_BRANCH,
            title=f"Creating {source_branch}",
            remove_source_branch=True,
            squash=False,
            labels=["create", "helm"],
            description=f"Automated MR for `{project_id}`",
        )
        logger.info("Helm MR ready: %s", getattr(mr, "web_url", "<no-url>"))
        return {**prior, "mr_iid": mr.iid, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        raise
    except Exception as e:
        # Transient network/api issues → retry with backoff
        raise self.retry(countdown=60, args=[prior], exc=e)


@shared_task(bind=True, name="workers.tasks.create.merge_helm_values_mr",
             queue="helm", retry_backoff=3, retry_backoff_max=60,
             retry_jitter=True, retry_kwargs={"max_retries": 6})
@track_step
def merge_helm_mr(self, prior:dict, **kwargs):
    project_id = prior.get("project_id")
    gl = gitlab.Gitlab(GITLAB_BASE_URL, private_token=GITLAB_TOKEN)
    project = gl.projects.get(GITLAB_HELM_PROJECT_ID)
    source_branch = f"{GIT_BRANCH_PREFIX}{project_id}"
    try:
        merge_results = merge_branch(
            retry=self.retry,
            project=project,
            source_branch=source_branch,
            target_branch=GIT_HELM_BASE_BRANCH,
            rebase_wait_seconds=GIT_REBASE_WAIT_SECONDS,
            log_prefix="[merge_helm_mr]",
        )
        return {**prior, "merge_results": merge_results, "workflow_id": prior["workflow_id"]}
    except CeleryRetry:
        # Let Celery handle the scheduled retry
        raise
    except Exception as e:
        # Catch-all: schedule another retry with context
        raise self.retry(countdown=60, args=[prior], exc=RuntimeError(f"Unexpected error: {e}"))