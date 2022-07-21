# Instructions
# Define a function that uses the python logger to log a function. Then finish filling in the details of the DAG down below. Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file or the video walkthrough on the next page.

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


#
# TODO: Define a function for the PythonOperator to call and have it log something
#
def hello_world():
    logging.info("Hello world!")

def addition():
    logging.info(f"2 + 2 = {2+2}")


def subtraction():
    logging.info(f"6 -2 = {6-2}")


def division():
    logging.info(f"10 / 2 = {int(10/2)}")

dag = DAG(
        'operator',
        start_date=datetime.datetime.now(),
        schedule_interval='@daily')

#
# TODO: Uncomment the operator below and replace the arguments labeled <REPLACE> below
#

hello_world = PythonOperator(
   task_id="hello_world",
   python_callable=hello_world,
   dag=dag
)

addition = PythonOperator(
    task_id='addition',
    python_callable = addition,
    dag = dag
)
subtraction = PythonOperator(
    task_id='subtraction',
    python_callable = subtraction,
    dag = dag
)
division = PythonOperator(
    task_id='division',
    python_callable = division,
    dag = dag
)
hello_world>>[addition,subtraction]>>division