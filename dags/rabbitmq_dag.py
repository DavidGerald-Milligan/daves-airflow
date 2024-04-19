import logging

from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor

task_logger = logging.getLogger("airflow.task")

@dag(
    dag_id="rabbitmq_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    default_args={"owner": "airflow", "retries": 3},
    catchup=False,
)
def rabbitmq_dag():
    rabbit_sensor = RabbitMQSensor(
        task_id="rabbitmq_sensor",
        rabbitmq_conn_id="rabbitmq_default",
        queue_name="test"
    )
    
    done_notification = EmptyOperator(task_id="done")
    
    chain(rabbit_sensor, done_notification)
    
rabbitmq_dag()