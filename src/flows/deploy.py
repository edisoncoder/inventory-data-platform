"""
Prefect deployment: builds and serves the flow.
Replaces cron: python this file to schedule natively with UI/monitoring.
"""
from prefect.deployments import Deployment
from .inventory_flow import inventory_flow
from ..config import get_config()

config = get_config()

inventory_deployment = Deployment.build_from_flow(
    flow=inventory_flow,
    name="inventory-daily-ingestion",
    schedule={"cron": config["schedule_cron"]},
    tags=["inventory", "v0.2.0"],
    description="Daily inventory CSV to Postgres pipeline",
    work_queue_name="default",
)

if __name__ == "__main__":
    inventory_deployment.serve()