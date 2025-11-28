# workers/database/db_workflow_helper.py

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional
from uuid import UUID

from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Connection

from workers.config.db_config import engine

logger = logging.getLogger(__name__)

# ----- utilities --------------------------------------------------------------

TERMINAL_WORKFLOW_STATUSES = {"success", "failed", "canceled"}
VALID_WORKFLOW_STATUSES    = {"pending", "running", "waiting"} | TERMINAL_WORKFLOW_STATUSES
TERMINAL_STEP_STATUSES     = {"success", "failed", "skipped"}   # adjust if you use different labels

def _coerce_uuid(val) -> Optional[str]:
    if val is None:
        return None
    try:
        return str(val)
    except Exception:
        return None

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

# ----- public API -------------------------------------------------------------

def _normalize_wid(wid):
    # accept wid, or {'workflow_id': wid}
    if isinstance(wid, dict):
        wid = wid.get("workflow_id")
    if isinstance(wid, UUID):
        return str(wid)
    if isinstance(wid, str):
        return wid
    raise TypeError(f"workflow_id must be UUID/str, got {type(wid).__name__}")


def expected_steps(conn, workflow_id):
    wid = _normalize_wid(workflow_id)
    row = conn.execute(
        text("SELECT expected_steps FROM public.workflows WHERE workflow_id = :wid"),
        {"wid": wid},
    ).first()
    return row[0] if row else None

def ensure_workflow_row(
    workflow_id: str,
    project_id: str,
    *,
    kind: str = "provisioner",
    expected_steps: int | None = None,
    status: str = "running",
) -> None:
    """
    Make sure a row exists in public.workflows for this workflow_id.
    Does nothing if it already exists.
    """
    if status not in VALID_WORKFLOW_STATUSES:
        raise ValueError(f"Invalid workflow status '{status}'")

    now = utcnow()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO public.workflows (
                    workflow_id, project_id, kind, status, expected_steps, started_at, updated_at
                )
                VALUES (
                    :wid, :pid, :kind, :status, :expected, :now, :now
                )
                ON CONFLICT (workflow_id) DO NOTHING
            """),
            {
                "wid":       workflow_id,
                "pid":       project_id,
                "kind":      kind,
                "status":    status,
                "expected":  expected_steps,
                "now":       now,
            }
        )


def set_workflow_status(
    workflow_id: str,
    status: str,
    *,
    current_step: Optional[str] = None,
    last_error: Optional[str]  = None,
    note: Optional[str]        = None,
    mark_ended: Optional[bool] = None,
) -> None:
    """
    Update the workflow's status and optional metadata.
    - If status is terminal OR mark_ended=True, sets ended_at=NOW().
    """
    if status not in VALID_WORKFLOW_STATUSES:
        raise ValueError(f"Invalid workflow status '{status}'")

    now = utcnow()
    ended = (status in TERMINAL_WORKFLOW_STATUSES) or bool(mark_ended)

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE public.workflows
                   SET status       = :status,
                       current_step = COALESCE(:current_step, current_step),
                       last_error   = COALESCE(:last_error, last_error),
                       note         = COALESCE(:note, note),
                       updated_at   = :now,
                       ended_at     = CASE WHEN :ended THEN :now ELSE ended_at END
                 WHERE workflow_id = :wid
            """),
            {
                "status":       status,
                "current_step": current_step,
                "last_error":   last_error,
                "note":         note,
                "now":          now,
                "ended":        ended,
                "wid":          workflow_id,
            }
        )


def append_step_event(
    workflow_id: str,
    step_name: str,
    *,
    task_id: str,
    queue: Optional[str],
    status: str,
    details: Optional[dict[str, Any]] = None,
    note: Optional[str] = None,
    mark_finished: bool = False,
    started_at: Optional[datetime] = None,
) -> None:
    """
    Upsert a row in workflow_steps keyed by (workflow_id, step_name, task_id).
    - Merges `details` JSONB.
    - Sets started_at if first time or if explicitly provided.
    - Sets ended_at when mark_finished=True or status in TERMINAL_STEP_STATUSES.
    - Always bumps updated_at to NOW().
    """
    now = utcnow()
    # Treat 'running'/'started' as "provide started_at" if not set
    if started_at is None and status in ("running"):
        started_at = now

    # Determine if we should set ended_at now
    will_end = mark_finished or (status in TERMINAL_STEP_STATUSES)

    stmt = text("""
        INSERT INTO public.workflow_steps (
            workflow_id, step_name, task_id, queue, status,
            started_at, ended_at, updated_at, details, note
        )
        VALUES (
            :wid, :step, :tid, :queue, :status,
            :started_at, NULL, :now, :details, :note
        )
        ON CONFLICT (workflow_id, step_name, task_id)
        DO UPDATE SET
            status     = EXCLUDED.status,
            queue      = COALESCE(EXCLUDED.queue, public.workflow_steps.queue),
            started_at = COALESCE(public.workflow_steps.started_at, EXCLUDED.started_at),
            ended_at   = CASE WHEN :mark_finished THEN :now
                              WHEN EXCLUDED.status = ANY(:terminal_statuses) THEN :now
                              ELSE public.workflow_steps.ended_at
                         END,
            details    = COALESCE(public.workflow_steps.details, '{}'::jsonb)
                          || COALESCE(EXCLUDED.details, '{}'::jsonb),
            note       = COALESCE(EXCLUDED.note, public.workflow_steps.note),
            updated_at = :now
    """).bindparams(
        bindparam("details", type_=JSONB),
        bindparam("terminal_statuses", value=list(TERMINAL_STEP_STATUSES))
    )

    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                "wid":            workflow_id,
                "step":           step_name,
                "tid":            task_id,
                "queue":          queue,
                "status":         status,
                "started_at":     started_at,
                "now":            now,
                "details":        details or {},
                "note":           note,
                "mark_finished":  bool(mark_finished),
            }
        )


def set_step_status(
    workflow_id: str,
    step_name: str,
    *,
    status: str,
    task_id: Optional[str] = None,
    note: Optional[str]    = None,
    details: Optional[dict[str, Any]] = None,
    mark_finished: bool = False,
) -> int:
    """
    Update an existing step row's status.
    - If task_id is provided, it targets that exact row.
    - Otherwise, it targets the most recently started row for the step.
    Returns the number of rows updated.
    """
    now = utcnow()
    will_end = mark_finished or (status in TERMINAL_STEP_STATUSES)

    base_params = {
        "wid":           workflow_id,
        "step":          step_name,
        "status":        status,
        "note":          note,
        "now":           now,
        "mark_finished": bool(mark_finished),
        "details":       details or {},
    }

    if task_id:
        sql = text("""
            UPDATE public.workflow_steps
               SET status     = :status,
                   note       = COALESCE(:note, note),
                   details    = COALESCE(details, '{}'::jsonb) || COALESCE(:details, '{}'::jsonb),
                   ended_at   = CASE WHEN :mark_finished OR :status = ANY(:terminal_statuses) THEN :now
                                     ELSE ended_at END,
                   updated_at = :now
             WHERE workflow_id = :wid
               AND step_name   = :step
               AND task_id     = :tid
        """).bindparams(
            bindparam("details", type_=JSONB),
            bindparam("terminal_statuses", value=list(TERMINAL_STEP_STATUSES))
        )
        params = dict(base_params, tid=task_id)
    else:
        # Update the latest started occurrence for that step
        sql = text("""
            UPDATE public.workflow_steps ws
               SET status     = :status,
                   note       = COALESCE(:note, ws.note),
                   details    = COALESCE(ws.details, '{}'::jsonb) || COALESCE(:details, '{}'::jsonb),
                   ended_at   = CASE WHEN :mark_finished OR :status = ANY(:terminal_statuses) THEN :now
                                     ELSE ws.ended_at END,
                   updated_at = :now
             WHERE ws.workflow_id = :wid
               AND ws.step_name   = :step
               AND ws.started_at  = (
                    SELECT MAX(started_at)
                      FROM public.workflow_steps
                     WHERE workflow_id = :wid
                       AND step_name   = :step
               )
        """).bindparams(
            bindparam("details", type_=JSONB),
            bindparam("terminal_statuses", value=list(TERMINAL_STEP_STATUSES))
        )
        params = base_params

    with engine.begin() as conn:
        res = conn.execute(sql, params)
        return res.rowcount or 0


def count_steps(
    workflow_id: str,
    *,
    statuses: Optional[Iterable[str]] = None,
) -> int:
    """
    Count steps that are in one of the provided statuses (defaults to terminal).
    """
    if statuses is None:
        statuses = tuple(TERMINAL_STEP_STATUSES)
    statuses = tuple(statuses)

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT COUNT(*)::int AS c
                  FROM public.workflow_steps
                 WHERE workflow_id = :wid
                   AND status = ANY(:statuses)
            """),
            {"wid": workflow_id, "statuses": list(statuses)}
        ).one_or_none()

    return int(row.c if row else 0)


def get_expected_steps(workflow_id: str) -> Optional[int]:
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT expected_steps FROM public.workflows WHERE workflow_id = :wid"),
            {"wid": workflow_id}
        ).one_or_none()
    return None if not row else row.expected_steps


def get_workflow_progress(workflow_id: str) -> dict[str, Any]:
    """
    Returns:
      {
        "workflow_id": ...,
        "expected": int|None,
        "done": int,
        "running": int,
        "failed": int,
        "success": int
      }
    """
    with engine.begin() as conn:
        w = conn.execute(
            text("SELECT expected_steps FROM public.workflows WHERE workflow_id = :wid"),
            {"wid": workflow_id}
        ).one_or_none()
        steps = conn.execute(
            text("""
                SELECT
                  SUM(CASE WHEN status IN ('success','failed','skipped') THEN 1 ELSE 0 END)::int AS done,
                  SUM(CASE WHEN status IN ('running','started') THEN 1 ELSE 0 END)::int AS running,
                  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)::int AS failed,
                  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::int AS success
                FROM public.workflow_steps
                WHERE workflow_id = :wid
            """),
            {"wid": workflow_id}
        ).one()
    return {
        "workflow_id": workflow_id,
        "expected": None if not w else w.expected_steps,
        "done": steps.done or 0,
        "running": steps.running or 0,
        "failed": steps.failed or 0,
        "success": steps.success or 0,
    }


def maybe_complete_workflow(workflow_id: str) -> bool:
    """
    If expected_steps is set and the number of terminal steps >= expected_steps,
    mark the workflow 'completed' (if not already terminal). Returns True if updated.
    """
    expected = get_expected_steps(workflow_id)
    if expected is None:
        return False

    done = count_steps(workflow_id, statuses=TERMINAL_STEP_STATUSES)
    if done < expected:
        return False

    # Try to mark completed; don't overwrite a failure/canceled status.
    with engine.begin() as conn:
        updated = conn.execute(
            text("""
                UPDATE public.workflows
                   SET status = CASE
                                  WHEN status = ANY(:terminal) THEN status
                                  ELSE 'completed'
                                END,
                       ended_at   = COALESCE(ended_at, :now),
                       updated_at = :now
                 WHERE workflow_id = :wid
            """),
            {"wid": workflow_id, "now": utcnow(), "terminal": list(TERMINAL_WORKFLOW_STATUSES)}
        ).rowcount or 0

    return updated > 0

def touch_workflow(workflow_id, *, current_step: Optional[str] = None, note: Optional[str] = None):
    wid = _coerce_uuid(workflow_id)
    if not wid:
        return
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE public.workflows
                   SET updated_at   = NOW(),
                       current_step = COALESCE(:current_step, current_step),
                       note         = COALESCE(:note, note)
                 WHERE workflow_id = :wid
            """),
            {"wid": wid, "current_step": current_step, "note": note},
        )
