from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {'owner': 'airflow', 'start_date': datetime(2021, 1, 1)}

dag = DAG('hello_world', schedule_interval='@daily', default_args=default_args, catchup=False)

def hello_world_py():
    print('Hello World')

with dag:
    t0 = DockerOperator(
                task_id='docker_command',
                image='centos:latest',
                api_version='auto',
                auto_remove=True,
                command="/bin/sleep 30",
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge"
        )

    # t1 = BashOperator(
    #     task_id="start_spark_cluster",
    #     bash_command="  ",
    # )
    # t2 = PythonOperator(
    #     task_id='hello_world',
    #     python_callable=hello_world_py)