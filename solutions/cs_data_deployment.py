from prefect.deployments import DeploymentSpec, SubprocessFlowRunner
from prefect.orion.schemas.schedules import IntervalSchedule
from datetime import timedelta

DeploymentSpec(
    name="cs_data_deployment",
    flow_location="./cs_data_flow.py",
    flow_name="collect-traffic-speed-by-cardinal-direction",
    flow_runner=SubprocessFlowRunner(),
    tags=['traffic','college-station'],
    parameters={'key': 'g7k6-2zz6'},
    schedule=IntervalSchedule(interval=timedelta(minutes=5))
)