import json
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': '310',
    'start_date': datetime(2024, 1, 22),
    # 'email': email_list,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
}

dag = DAG(
    dag_id='report_shk_return_310',
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

def main():

    dt_max = client.execute(f"""
            (select max(dt_ocr) from report.shk_return_310) 
        """)[0][0]

    client.execute(f"""drop table if exists tmp.table11_1_310;""")

    create_tmp_1 = f"""
        create table tmp.table11_1_310 ENGINE = Memory() as
        select item_id shk_id, dt dt_ocr
        from history.ShkOnPlace
        where (dt >= {dt_max} - interval 1 hour 
            or shk_id in (
                select shk_id
                   from report.shk_return_310
                   where dt_repack = 0 or dt_return = 0
            ))
            and state_id = 'OCR'
    """

    client.execute(create_tmp_1)

    client.execute(f"""drop table if exists tmp.table11_2_310;""")

    create_tmp_2 = f"""
        create table tmp.table11_2_310 ENGINE = Memory() as
        select rid_hash, shk_id, min(dt) dt_assembly
        from history.assembled
        where dt >= toStartOfDay(now()) - interval 30 day 
            and shk_id in (select shk_id from tmp.table11_1_310)
        group by rid_hash, shk_id
    """

    client.execute(create_tmp_2)

    client.execute(f"""drop table if exists tmp.table11_3_310;""")

    create_tmp_3 = f"""
        create table tmp.table11_3_310 ENGINE = Memory() as
        select rid_hash, dt_ocr, shk_id
        from tmp.table11_2_310 t2
        asof join tmp.table11_1_310 t1
        on t1.shk_id = t2.shk_id and t1.dt_ocr > t2.dt_assembly
    """

    client.execute(create_tmp_3)

    client.execute(f"""drop table if exists tmp.table11_4_310;""")

    create_tmp_4 = f"""
        create table tmp.table11_4_310 ENGINE = Memory() as
        select item_id shk_id, dt dt_mx, mx, state_id, employee_id
            , dictGet('dictionary.StoragePlace','office_id', toUInt64(mx)) office_id
        from history.ShkOnPlace
        where dt >= toStartOfDay(now()) - interval 30 day
            and item_id in (select shk_id from tmp.table11_3_310)
            and (state_id = 'WPU' or dictGet('dictionary.BranchOffice', 'type_point', toUInt64(office_id)) = 13);
    """

    client.execute(create_tmp_4)

    client.execute(f"""drop table if exists tmp.table11_5_310;""")

    create_tmp_5 = f"""
        create table tmp.table11_5_310 ENGINE = Memory() as
        select rid_hash, shk_id, dt_ocr, dt_mx, mx, state_id, employee_id
        from tmp.table11_4_310 t4
        asof join tmp.table11_3_310 t3
        on t4.shk_id = t3.shk_id and t4.dt_mx > t3.dt_ocr
    """

    client.execute(create_tmp_5)

    client.execute(f"""drop table if exists tmp.table11_6_310;""")

    create_tmp_6 = f"""
        create table tmp.table11_6_310 ENGINE = MergeTree order by (rid_hash) as
        select rid_hash, shk_id, src_office_id, dt_ocr, dt_mx, mx, state_id, employee_id
            , dictGet('dictionary.StoragePlace','office_id', toUInt64(mx)) office_id
        from tmp.table11_5_310 t5
        left join
        (
            select rid_hash, src_office_id
            from history.assembly_task_issued
            where issued_dt >= now() - interval 30 day
                and rid_hash in (select rid_hash from tmp.table11_5_310)
        ) ati
        on t5.rid_hash = ati.rid_hash
    """

    client.execute(create_tmp_6)

    insert_query =  f"""
        insert into report.shk_return_310
        select src_office_id
            , rid_hash
            , shk_id
            , dt_ocr
            , minIf(dt_mx, office_id = src_office_id) dt_return
            , minIf(dt_mx, state_id = 'WPU') dt_repack
            , argMinIf(office_id, dt_mx, state_id = 'WPU') repack_office_id
            , argMinIf(mx, dt_mx, state_id = 'WPU') mx_repack
        from tmp.table11_6_310
        group by rid_hash, shk_id, dt_ocr, src_office_id
        having dt_repack != 0 or dt_return != 0
    """

    client.execute(insert_query)


task1 = PythonOperator(
    task_id='report_shk_return_310', python_callable=main, dag=dag)

