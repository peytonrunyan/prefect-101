from prefect.deployments import DeploymentSpec, SubprocessFlowRunner

DeploymentSpec(
    name="my-first-deployment",
    flow_location="./trivial-flow.py",
    flow_name="Previously unreliable pipeline",
    parameters={'msg':'Hello from my first deployment!'},
    tags=['ETL'],
    flow_runner=SubprocessFlowRunner()
)