from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from datetime import datetime, timedelta
import json

def delete_tables():
    # Establish a connection to the PostgreSQL database
    with open('/opt/airflow/data/settings.json', 'r') as file:
        settings = json.load(file)
    conn = psycopg2.connect(
        host = settings["HOST"],
        port = settings["PORT"],
        database = settings["DATABASE"],
        user = settings["USER"],
        password = settings["PASSWORD"]
    )
    cur = conn.cursor()

    # Delete table1
    cur.execute('DROP TABLE IF EXISTS nydp_arrest_data')

    # Delete table2
    cur.execute('DROP TABLE IF EXISTS predictions_bronx_m')

    # Delete table3
    cur.execute('DROP TABLE IF EXISTS predictions_bronx_f')

    # Delete table4
    cur.execute('DROP TABLE IF EXISTS predictions_bronx_v')

    # Delete table5
    cur.execute('DROP TABLE IF EXISTS predictions_manhatan_m')

    # Delete table6
    cur.execute('DROP TABLE IF EXISTS predictions_manhatan_f')

    # Delete table7
    cur.execute('DROP TABLE IF EXISTS predictions_manhatan_v')

    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'retries': 1,
}

with DAG(
    dag_id='proc_delete_tables',
    start_date = datetime(2023, 1, 1),
    schedule = '@daily', 
    catchup = False,
    ) as dag:

    delete_tables_task = PythonOperator(
        task_id='delete_tables_task',
        python_callable=delete_tables,
        dag=dag
    )

    delete_tables_task
