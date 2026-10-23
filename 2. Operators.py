from airflow.sdk import dag, task 
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s

@dag(
        dag_id="operators_dag",
)
def operators_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")
    
    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org/"

    bash_task_oldschool = BashOperator(
    task_id="bash_task_oldschool",
    bash_command="echo https://airflow.apache.org/",
    )

    # Runs in its own pod from a plain Python image - no Airflow inside.
    # The Airflow task pod launches this pod, streams its output into the
    # task log, and succeeds or fails on the container's exit code.
    # Uses the kubernetes_default connection (in-cluster, airflow-tasks).

    @task.kubernetes(
        task_id="pod_task",
        name="pod-task",
        namespace="airflow-tasks",
        image="python:3.13-alpine",
        env_vars={"GREETING": "hi from Airflow"},
        # Downward API: tell the container which node it landed on.
        pod_runtime_info_envs=[
            k8s.V1EnvVar(
                name="NODE_NAME",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="spec.nodeName")
                ),
            )
        ],
        node_selector={"homelab.local/cpu": "true"},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "64Mi"},
            limits={"memory": "128Mi"},
        ),
        get_logs=True,
        on_finish_action="delete_pod",
    )
    def pod_task():
        import os, platform, socket
        print("Hello from", socket.gethostname(), "on node", os.environ["NODE_NAME"])
        print("Python", platform.python_version(), "| greeting:", os.environ["GREETING"])

    
    
    # Defining task dependencies
    first = first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    bash_oldschool = bash_task_oldschool
    pod = pod_task()

    first >> second >> bash_modern >> bash_oldschool >> pod

# Instantiating the DAG
operators_dag()
