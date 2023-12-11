import datetime
import json
from clickhouse_driver import Client

dbname = 'agg'
src_table = 'history.calc'
dst_table = 'agg.calc_by_dth_emp_310'


with open('secrets/ch.json') as json_file:
    data = json.load(json_file)

client = Client(data['server'][0]['host'],
                user=data['server'][0]['user'],
                password=data['server'][0]['password'],
                port=data['server'][0]['port'],
                verify=False,
                database=dbname,
                settings={"numpy_columns": False, 'use_numpy': False},
                compression=True)

for i in range(1, 31):

    print(f"Итерация: {i}. Обрабатываются заказы: за {i} день.")

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
        where dt between now() - interval {i} day and now() - interval {i -1} day
        group by prodtype_id, dt_h, dt_h_msk, employee_id, office_id, calc_date
        """

    client.execute(insert_query)

print(f"Таблица {dst_table} заполнена.")