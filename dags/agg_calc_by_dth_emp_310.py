import json
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

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
    dag_id='agg_calc_by_dth_emp_310',
    default_args=default_args,
    schedule_interval='12,32,52 * * * *',
    
    # напиши ещё одним вариантом промежуток
    
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

dbname = 'agg'
src_table = 'history.calc'
dst_table = 'agg.calc_by_dth_emp_310'

def main():

    insert_query = f"""
        insert into {dst_table}
        select office_id 
        , employee_id 
        , toStartOfHour(dt) dt_h 	
        , toStartOfHour(msk_dt) dt_h_msk 
        , prodtype_id 
        , count(dt) qty_oper 
        , sum(amount) amount 
        , calc_date
    from {src_table}
    where dt_h_msk >= (select max(dt_h_msk) from {dst_table})
    -- final забыл
    -- зачем нам final если есть max? 
    -- нужно последнее состояние таблицы и при работе с replacing лучше всегда укзывать final
    group by prodtype_id, dt_h, dt_h_msk, employee_id, office_id, calc_date
    """

    client.execute(insert_query)
    print(f"Таблица обновлена")

task1 = PythonOperator(
    task_id='agg_calc_by_dth_emp_310', python_callable=main, dag=dag)

# Почему не используем выполнение дагов во время, которое кратно 5 минутам.
# Почему исключаем диапазон рядом с началом каждого часа.

# Мы не используем это время, потому что оно очень популярное.
# То есть в это время могут одновременно работать несколько дагов, и они будут сильно грузить сервер.
