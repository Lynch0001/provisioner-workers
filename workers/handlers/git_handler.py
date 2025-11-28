# githandler.py
from __future__ import annotations

import logging
import os, shutil, tempfile
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Mapping, Iterable

from git import Repo, GitCommandError  # pip install GitPython

from workers.config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class GitRepoSpec:
    repo_url: str
    base_branch: str = "main"
    branch_prefix: str = "provision/"
    workdir_base: Optional[str] = None
    depth: int = 1  # shallow clone

class GitHandler:
    """
    Helper to clone, branch, edit files, commit, and push.
    Framework-agnostic: no Celery/Flask/FastAPI imports here.
    Use in a `with` block so it auto-cleans the temp dir.
    """
    def __init__(self, spec: GitRepoSpec, project_id: str, *, cleanup: bool = True):
        self.spec = spec
        self.project_id = project_id
        self.cleanup_enabled = cleanup
        self._tmpdir: Optional[str] = None
        self.repo: Optional[Repo] = None
        self.branch_name: Optional[str] = None

    # ---------- context ----------
    def __enter__(self) -> "GitHandler":
        base = self.spec.workdir_base or tempfile.gettempdir()
        self._tmpdir = tempfile.mkdtemp(prefix=f"{self.project_id}-", dir=base)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.cleanup_enabled and self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    # ---------- paths ----------
    @property
    def workdir(self) -> Path:
        if not self._tmpdir:
            raise RuntimeError("GitHandler not entered; use `with GitHandler(...)`")
        return Path(self._tmpdir)

    @property
    def root(self) -> Path:
        if not self.repo:
            raise RuntimeError("Repo not initialized; call init_repo() first")
        return Path(self.repo.working_tree_dir)

    # ---------- git ops ----------
    def init_repo(self) -> None:
        self.repo = Repo.clone_from(
            self.spec.repo_url,
            str(self.workdir),
            branch=self.spec.base_branch,
            depth=self.spec.depth,
        )

    def create_branch(self, *, explicit_name: Optional[str] = None) -> str:
        if not self.repo:
            raise RuntimeError("Repo not initialized; call init_repo() first")
        name = explicit_name or f"{self.spec.branch_prefix}{self.project_id}"
        self.repo.git.checkout("HEAD", b=name)
        self.branch_name = name
        return name

    def remote_branch_exists(self, branch: Optional[str] = None) -> bool:
        if not self.repo:
            raise RuntimeError("Repo not initialized")
        branch = branch or self.branch_name
        if not branch:
            raise RuntimeError("No branch name set")
        try:
            # cheap check via ls-remote
            for ref in self.repo.git.ls_remote("--heads", "origin", branch).splitlines():
                if ref.strip():
                    return True
        except GitCommandError:
            return False
        return False

    def add_all(self) -> None:
        if not self.repo:
            raise RuntimeError("Repo not initialized")
        self.repo.git.add(all=True)

    def commit(self, message: str) -> None:
        if not self.repo:
            raise RuntimeError("Repo not initialized")
        # Optionally set identity if your runner lacks it:
        # with self.repo.config_writer() as cw:
        #     cw.set_value("user", "name", "Provisioner Bot")
        #     cw.set_value("user", "email", "provisioner@example.com")
        self.repo.index.commit(message)

    def push(self, *, branch: Optional[str] = None, force: bool = False) -> None:
        # Observe what Celery actually delivered
        logger.info(f"attempting to push branch {branch}")
        if not self.repo:
            raise RuntimeError("Repo not initialized")
        branch = branch or self.branch_name
        if not branch:
            raise RuntimeError("No branch to push")
        origin = self.repo.remote(name="origin")
        try:
            if force:
                origin.push(f"+{branch}:{branch}")
            else:
                origin.push(branch)
        except GitCommandError as e:
            raise RuntimeError(f"Git push failed: {e}")

    def commit_and_push(self, message: str, *, force: bool = False) -> None:
        self.add_all()
        self.commit(message)
        self.push(force=force)

    # ---------- fs helpers ----------
    def copy_tree(self, src_rel: str | Path, dst_rel: str | Path, *, dirs_exist_ok=True) -> None:
        src = self.root / src_rel
        dst = self.root / dst_rel
        if not src.exists():
            raise FileNotFoundError(f"Source path not found: {src}")
        shutil.copytree(src, dst, dirs_exist_ok=dirs_exist_ok)

    def mkdir(self, dst_rel: str | Path) -> None:
        dst = self.root / dst_rel
        dst.mkdir(parents=True, exist_ok=True)
        if dst.exists() and dst.is_dir():
            print(f"Directory {dst} created")
        else:
            raise NotADirectoryError(f"Directory {dst} does not exist")

    def copy(self, src_rel: str | Path, dst_rel: str | Path) -> None:
        src = self.root / src_rel
        dst = self.root / dst_rel
        if not src.exists():
            raise FileNotFoundError(f"Source path not found: {src}")
        shutil.copy(src, dst)

    def replace_tokens_in_file(self, file_rel: str | Path, replacements: Mapping[str, str]) -> None:
        fp = self.root / file_rel
        if not fp.exists():
            raise FileNotFoundError(f"File not found: {fp}")
        text = fp.read_text()
        for old, new in replacements.items():
            text = text.replace(old, new)
        fp.write_text(text)

    def replace_tokens_in_tree(
        self,
        root_rel: str | Path,
        replacements: Mapping[str, str],
        file_globs: Iterable[str] = ("*.hcl","*.tf","*.tfvars","*.yaml","*.yml","*.json"),
    ) -> int:
        base = self.root / root_rel
        if not base.exists():
            raise FileNotFoundError(f"Tree root not found: {base}")
        changed = 0
        for pattern in file_globs:
            for fp in base.rglob(pattern):
                orig = fp.read_text()
                new = orig
                for old, repl in replacements.items():
                    new = new.replace(old, repl)
                if new != orig:
                    fp.write_text(new)
                    changed += 1
        return changed


def _ensure_under_repo(repo_root: Path, target: Path):
    repo_root = repo_root.resolve()
    target = target.resolve()
    if repo_root not in target.parents and target != repo_root:
        raise RuntimeError(f"Refusing to delete outside repo: {target} not under {repo_root}")

def _on_rm_error(func, path, exc_info):
    # Make read-only files writable, then retry
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception:
        # Fall through; let rmtree raise later so caller can decide
        pass

def safe_delete_path(repo_root: Path, relative_path: str) -> dict:
    """
    Delete a path (file or dir) inside the git repo working tree and stage the change.
    Works whether files are tracked or untracked.

    Returns a dict with details for logging.
    """
    repo_root = Path(repo_root).resolve()
    rel = Path(relative_path)
    abs_target = (repo_root / rel).resolve()

    # 1) Safety: never delete outside repo
    _ensure_under_repo(repo_root, abs_target)

    # 2) If nothing to delete, that's not an error — just log and move on
    if not abs_target.exists():
        logger.info("[delete] nothing to delete: %s", rel.as_posix())
        return {"deleted": False, "reason": "not_found", "path": rel.as_posix()}

    # 3) Try `git rm -r --ignore-unmatch` first (handles tracked files & stages the deletion)
    try:
        subprocess.run(
            ["git", "rm", "-r", "--ignore-unmatch", rel.as_posix()],
            cwd=str(repo_root),
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deleted_via = "git_rm"
    except subprocess.CalledProcessError as e:
        logger.debug("[delete] git rm failed for %s: %s", rel.as_posix(), e)
        # 4) Fallback: filesystem delete (untracked files / directories), then stage all changes
        if abs_target.is_dir():
            shutil.rmtree(abs_target, onerror=_on_rm_error)
        else:
            try:
                os.chmod(abs_target, stat.S_IWRITE)
            except Exception:
                pass
            os.remove(abs_target)
        # Stage deletions
        subprocess.run(["git", "add", "-A"], cwd=str(repo_root), check=True)
        deleted_via = "fs_rm+git_add"

    # 5) Verify gone
    still_exists = abs_target.exists()
    if still_exists:
        raise RuntimeError(f"Failed to delete path: {rel.as_posix()}")

    return {"deleted": True, "path": rel.as_posix(), "via": deleted_via}