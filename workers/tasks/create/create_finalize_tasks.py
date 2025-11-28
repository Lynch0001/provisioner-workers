# destroy_finalize_tasks.py
import logging
from celery import shared_task
from workers.config.logging_config import setup_logging
from workers.database.db_workflow_helper import set_workflow_status, expected_steps, count_steps
from workers.config.db_config import engine
from workers.database.db_wrapper import track_step

setup_logging()
logger = logging.getLogger(__name__)

@shared_task(bind=True, name="workers.tasks.create.finalize_workflow", queue="provisioner")
@track_step
def finalize_workflow(self, prior: dict | None = None, **kwargs):
    wid = kwargs.get("workflow_id") or (prior or {}).get("workflow_id")
    if not wid:
        raise ValueError("finalize_workflow requires workflow_id")

   # TODO will never show failed (Complete vs Success)
    with engine.begin() as conn:
        exp = expected_steps(conn,wid)
        print("##################: Expected Steps {} steps".format(exp))
        done = count_steps(wid, statuses=("success","failed"))
        print("##################: Done {} steps".format(done))
        if exp is None:
            set_workflow_status(wid, "unknown", note="No expected_steps row")
        elif done >= exp:
            set_workflow_status(wid, "success")
        else:
            set_workflow_status( wid, "running", note=f"{done}/{exp} steps finished")

    # ensure wid flows forward if anything else listens
    return {"workflow_id": str(wid), "final": True}