# hello.py
from airflow.sdk import dag, task
import pendulum

@dag(schedule=None, start_date=pendulum.datetime(2026, 1, 1), catchup=False)
def hello():
    @task
    def say():
        print("hello from the homelab")
    say()

hello()