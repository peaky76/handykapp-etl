from rapid_horseracing import rapid_horseracing_extractor
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=rapid_horseracing_extractor,
    name="standard",
    parameters={},
    schedule={
        "cron": "1-50 1 * * *",
        "timezone": "Europe/London",
        "day_or": "true"
    },
    infra_overrides={},
    work_queue_name="default",
)

if __name__ == "__main__":
    deployment.apply()
