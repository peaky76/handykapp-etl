from bha import bha_extractor
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=bha_extractor,
    name="standard",
    parameters={},
    schedule={
        "cron": "0 23 * * 2",
        "timezone": "Europe/London",
        "day_or": "true"
    },
    infra_overrides={},
    work_queue_name="default",
)

if __name__ == "__main__":
    deployment.apply()

# COMMENT CHANGE TO PROVOKE MASTER MERGE AND REMOTE TEST