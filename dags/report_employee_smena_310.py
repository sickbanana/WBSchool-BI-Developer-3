import json
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': '310',
    'start_date': datetime(2023, 12, 17),
    # 'email': email_list,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
}

dag = DAG(
    dag_id='report_employee_smena_310',
    default_args=default_args,
    schedule_interval='22 * * * *',
    description='Смены сотрудников.',
    catchup=False,
    max_active_runs=1,
    tags=["310"]  # Поставить свой номер.
)

with open('/opt/airflow/dags/secret/ch.json') as json_file:
    data = json.load(json_file)

client = Client(data['server'][0]['host'],
                user=data['server'][0]['user'],
                password=data['server'][0]['password'],
                port=data['server'][0]['port'],
                verify=False,
                database='default',
                settings={"numpy_columns": False, 'use_numpy': False},
                compression=True)

turniket_table = 'history.turniket'
tmp_table = 'tmp.temp_smena'
src_table = 'agg.calc_by_dth_emp_310'
dst_table = 'report.employee_smena_310'


def main():
    delete_query = """
        drop table if exists {tmp_table}
            """
# a  где собственно client.execute ??
# рассмотри вариант сразу писать запросы в client.execute, а не через объявление переменных

    print(f'Временная таблица {tmp_table} удаленна')

    create_table_query = f"""
        create table {tmp_table} ENGINE = Memory as
        select employee_id
            , dt dt_smena_start
            , if(any(dt_prev) over (rows between 1 following and 1 following) as dt_next = '1970-01-01 00:00:00'
                    or any(is_in_prev) over (rows between 1 following and 1 following) = 1
                    or any(employee_id) over (rows between 1 following and 1 following) != employee_id
                , dt_smena_start + interval 12 hour, dt_next) dt_smena_end
            , if(dt_smena_start + interval round(date_diff('hour', dt_smena_start, dt_smena_end) / 2) hour between toStartOfDay(dt_smena_start) + interval 8 hour
                    and toStartOfDay(dt_smena_start) + interval 20 hour
                , 1, 0) sm_type
        from
        (
            select employee_id, dt, is_in
            , any(dt) over (partition by employee_id order by dt rows between 1 preceding and 1 preceding) dt_prev
            , any(is_in) over (partition by employee_id order by dt rows between 1 preceding and 1 preceding) is_in_prev
            from {turniket_table}
            where dt >= (select max(dt_smena_start) from {dst_table} final) - interval 3 day
        )
        """

    client.execute(create_table_query)

    print(f'Создана временная таблица {tmp_table}')

    insert_query = f"""
        insert into {dst_table}
        select employee_id, office_id
            , toDate(dt_smena_start) dt_date
            , dt_smena_start, dt_smena_end, sm_type, prodtype_id
            , sum(qty_oper) qty_oper
            , sum(amount) amount
        from (
            select office_id, employee_id, dt_h, prodtype_id, qty_oper, amount
            from {src_table}
            where dt_h >= (select max(dt_smena_start) from {dst_table} final) - - interval 3 day + interval 3 hour
        ) a
        asof join {tmp_table} t
        on a.employee_id = t.employee_id and dt_h - interval 3 hour >= dt_smena_start
        group by employee_id, office_id, dt_smena_start, dt_smena_end, sm_type, prodtype_id
            """

    client.execute(insert_query)

    print(f'Таблица {src_table} обновлена')

task1 = PythonOperator(
    task_id='report_employee_smena_310', python_callable=main, dag=dag)
