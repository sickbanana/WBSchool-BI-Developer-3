import datetime
import json
from clickhouse_driver import Client

# 04. Заполнить витрину через Python за все дни.
# Заполнять итеративно по суткам.
# Сколько строк получилось.
# Скрипт выложить в гит.

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

query_len = 0

for i in range(1, 31):

    print(f"Итерация: {i}. Обрабатываются заказы: за {i} день.")

    select_query = f"""
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

    data = client.execute(select_query)

    client.execute(f"insert into {dst_table} values", data)

    query_len += len(data)

print(f"Добавлено {query_len} данных")
print(f"Таблица заполнена.")