# handlers/gitlab_mr_handler.py
from __future__ import annotations
import gitlab
from gitlab.exceptions import GitlabHttpError
from typing import Optional, Dict, Any

def get_gl_project(base_url: str, token: str, project_id: str):
    gl = gitlab.Gitlab(base_url, private_token=token)
    return gl.projects.get(project_id)

def get_or_create_mr(
    *,
    project,                       # gitlab project object
    source_branch: str,
    target_branch: str,
    title: str,
    draft: bool = False,
    remove_source_branch: bool = True,
    squash: bool = False,
    labels: Optional[list[str]] = None,
    description: Optional[str] = None,
    allow_collaboration: Optional[bool] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
):
    """
    Idempotent MR open-or-create.
    Returns the python-gitlab MR object.
    """
    # 1) Check if an OPEN MR already exists for these branches
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

    # 2) If none open, try to create a new MR
    payload = {
        "source_branch": source_branch,
        "target_branch": target_branch,
        "title": f"DRAFT: {title}" if draft else title,
        "remove_source_branch": remove_source_branch,
        "squash": squash,
    }
    if labels:
        payload["labels"] = ",".join(labels)
    if description:
        payload["description"] = description
    if allow_collaboration is not None:
        payload["allow_collaboration"] = allow_collaboration
    if extra_fields:
        payload.update(extra_fields)

    try:
        mr = project.mergerequests.create(payload)
        return mr
    except GitlabHttpError as e:
        # If another worker created it moments ago, fetch the latest open one again
        if getattr(e, "response_code", None) in (409, 422):
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
        raise