import json
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# email_list = ['123@123.ru']

default_args = {
    'owner': '310',
    'start_date': datetime(2023, 12, 10),
    # 'email': email_list,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
}

dag = DAG(
    dag_id='report_ridsOver8Hours_310',
    default_args=default_args,
    schedule_interval='42 * * * *',
    description='Заказы более 8 часов.',
    catchup=False,
    max_active_runs=1,
    tags=["310"]  # Поставить свой номер.
)

# Это не правим.
with open('/opt/airflow/dags/secret/ch.json') as json_file:
    data = json.load(json_file)

# Это не правим.
client = Client(data['server'][0]['host'],
                user=data['server'][0]['user'],
                password=data['server'][0]['password'],
                port=data['server'][0]['port'],
                verify=False,
                database='default',
                settings={"numpy_columns": False, 'use_numpy': False},
                compression=True)

# ---=== Меняем код ниже этого комментария. ===---

dbname = 'report'
src_table = 'current.ridHello'
dst_table = 'report.orders_not_in_assembly_310'


def main():

    delete_query = f"""
        insert into {dst_table}
        with
        (
            select max(dt) from {src_table}
        ) as dt_max_time
        select rid_hash
            , argMax(src_office_id, dt) src_office_id
            , argMax(dst_office_id, dt) dst_office_id
            , max(dt) dt_last
            , 1 is_deleted
        from {src_table}
        where dt >= (select max(dt_last) from {dst_table}) - interval 10 hour
            and rid_hash in (select rid_hash from {dst_table} final where is_deleted = 0)
        group by rid_hash
        having argMax(src, dt) != 'assembly_task'
            or dt_last > dt_max_time - interval 8 hour
        order by rid_hash
    """

    client.execute(delete_query)
    print(f"Добавлены данные с флагом is_deleted = 1")

    insert_query = f"""
        insert into {dst_table}
        with
        (
            select max(dt) from {src_table}
        ) as dt_max_time
        select rid_hash
            , argMax(src_office_id, dt) src_office_id
            , argMax(dst_office_id, dt) dst_office_id
            , max(dt) dt_last
            , 0 is_deleted
        from {src_table}
        where dt >= (select max(dt_last) from {dst_table} where is_deleted = 0) - interval 10 hour
            and rid_hash not in (select rid_hash from {dst_table} final where is_deleted = 0)
        group by rid_hash
        having argMax(src, dt) = 'assembly_task'
            and dt_last < dt_max_time - interval 8 hour
    """

    client.execute(insert_query)
    print(f"Добавлены новые данные")

task1 = PythonOperator(
    task_id='report_ridsOver8Hours_310', python_callable=main, dag=dag)