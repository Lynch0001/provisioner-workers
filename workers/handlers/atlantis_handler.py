# handlers/atlantis_handler.py
from __future__ import annotations
from typing import Optional, Dict, Any
import gitlab
from gitlab.exceptions import GitlabHttpError

def get_gl_project(base_url: str, token: str, project_id: str):
    gl = gitlab.Gitlab(base_url, private_token=token)
    return gl.projects.get(project_id)

def _find_latest_open_mr(project, source_branch: str, target_branch: str):
    mrs = project.mergerequests.list(
        source_branch=source_branch,
        target_branch=target_branch,
        state="opened",
        order_by="updated_at",
        sort="desc",
        all=True,
        per_page=1,
    )
    return project.mergerequests.get(mrs[0].iid) if mrs else None

def _build_atlantis_apply_cmd(*, workspace: Optional[str], project_name: Optional[str], directory: Optional[str], terraform_destroy: Optional[bool]) -> str:
    parts = ["atlantis", "apply"]
    if workspace:
        parts += ["-w", workspace]
    if project_name:
        parts += ["-p", project_name]
    if directory:
        parts += ["-d", directory]
    if terraform_destroy:
        parts += ["-- -destroy"]
    return " ".join(parts)

def post_atlantis_apply_comment(
    *,
    project,                      # python-gitlab project
    mr_iid: Optional[int] = None,
    source_branch: Optional[str] = None,
    target_branch: Optional[str] = None,
    workspace: Optional[str] = None,
    project_name: Optional[str] = None,
    directory: Optional[str] = None,
    terraform_destroy: bool = False,
) -> Dict[str, Any]:
    """
    Find (or load) the MR and post an 'atlantis apply' comment.
    Returns dict with mr_iid, mr_url, note_id, note_body.
    """
    if mr_iid is not None:
        mr = project.mergerequests.get(mr_iid)
    else:
        if not source_branch or not target_branch:
            raise ValueError("Either mr_iid OR (source_branch and target_branch) must be provided.")
        mr = _find_latest_open_mr(project, source_branch, target_branch)
        if not mr:
            raise RuntimeError(f"No open MR found from '{source_branch}' -> '{target_branch}'.")

    cmd = _build_atlantis_apply_cmd(workspace=workspace, project_name=project_name, directory=directory, terraform_destroy=terraform_destroy)

    try:
        note = mr.notes.create({"body": cmd})
    except GitlabHttpError as e:
        raise RuntimeError(f"Failed to create MR note: {e}") from e

    return {
        "mr_iid": mr.iid,
        "mr_url": mr.web_url,
        "note_id": note.id,
        "note_body": note.body,
    }