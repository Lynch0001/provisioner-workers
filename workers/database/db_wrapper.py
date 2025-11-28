# workers/database/db_wrapper.py
import functools
import logging
from datetime import datetime
from celery.exceptions import Retry as CeleryRetry

from workers.database.db_workflow_helper import (
    append_step_event,
    set_workflow_status,
    touch_workflow,
)

logger = logging.getLogger(__name__)

def _extract_workflow_id(args, kwargs):
    wid = kwargs.get("workflow_id")
    if wid:
        return wid
    if args:
        first = args[0]
        if isinstance(first, dict):
            return first.get("workflow_id")
    return None

def _extract_project_id(args, kwargs):
    pid = kwargs.get("project_id")
    if pid:
        return pid
    if args:
        first = args[0]
        if isinstance(first, dict):
            return first.get("project_id")
    return None

def _extract_queue(req) -> str | None:
    try:
        di = getattr(req, "delivery_info", None) or {}
        return di.get("routing_key") or di.get("queue")
    except Exception:
        return None

def track_step(task_func):
    """
    Decorator for Celery tasks (bind=True required).
    Records step start/success/failure AND updates workflow status accordingly.
    - start  -> workflow.status='running', current_step=<step>
    - success-> step success, workflow.status stays 'running'
    - failure-> step failed, workflow.status='failed', ended_at set
    """
    @functools.wraps(task_func)
    def wrapper(self, *args, **kwargs):
        step_name   = kwargs.get("step_name") or task_func.__name__
        workflow_id = _extract_workflow_id(args, kwargs)
        project_id  = _extract_project_id(args, kwargs)

        req     = getattr(self, "request", None)
        task_id = getattr(req, "id", None)
        queue   = _extract_queue(req)
        now_iso = datetime.utcnow().isoformat()

        # Mark workflow running + current step (non-fatal if DB write fails)
        if workflow_id:
            try:
                set_workflow_status(
                    workflow_id,
                    status="running",
                    current_step=step_name,
                )
            except Exception as e:
                logger.exception("[track_step] set_workflow_status(running) failed: %s", e)

            # Record step start
            try:
                append_step_event(
                    workflow_id=workflow_id,
                    step_name=step_name,
                    status="running",
                    task_id=task_id,
                    queue=queue,
                    details={
                        "event": "start",
                        "step": step_name,
                        "task_id": task_id,
                        "queue": queue,
                        "project_id": project_id,
                        "ts": now_iso,
                    },
                    started_at=datetime.utcnow(),
                )
            except Exception as e:
                logger.exception("[track_step] append_step_event(start) failed: %s", e)

        try:
            result = task_func(self, *args, **kwargs)

            if workflow_id:
                # Step success
                try:
                    append_step_event(
                        workflow_id=workflow_id,
                        step_name=step_name,
                        status="success",
                        task_id=task_id,
                        queue=queue,
                        details={"event": "success", "ts": datetime.utcnow().isoformat()},
                        mark_finished=True,
                    )
                except Exception as e:
                    logger.exception("[track_step] append_step_event(success) failed: %s", e)

                # Keep workflow running; just touch it (don’t set completed here)
                try:
                    touch_workflow(workflow_id, current_step=step_name)
                except Exception as e:
                    logger.exception("[track_step] touch_workflow failed: %s", e)

            return result

        except CeleryRetry:
            # On retry we do not flip workflow to failed
            raise

        except Exception as e:
            if workflow_id:
                # Step failure
                try:
                    append_step_event(
                        workflow_id=workflow_id,
                        step_name=step_name,
                        status="failed",
                        task_id=task_id,
                        queue=queue,
                        note=str(e),
                        details={"event": "error", "error": str(e), "ts": datetime.utcnow().isoformat()},
                        mark_finished=True,
                    )
                except Exception:
                    logger.exception("[track_step] append_step_event(failed) failed")

                # Mark workflow failed & ended
                try:
                    set_workflow_status(
                        workflow_id,
                        status="failed",
                        last_error=str(e),
                        ended=True,
                    )
                except Exception:
                    logger.exception("[track_step] set_workflow_status(failed) failed")

            raise
    return wrapper