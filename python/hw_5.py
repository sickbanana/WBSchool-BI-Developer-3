import datetime
import json
from clickhouse_driver import Client

dbname = 'report'
src_table = 'current.ridHello'
dst_table = 'report.orders_not_in_assembly_310'


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

# 04
# В питон скрипте донаписать генерацию последовательности для функции квантиль с нужным шагом. от 0.1 до 0.01.
# Это: (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9).
step = 0.05
quantiles = [(round(i * step, len(str(step)) - 1)) for i in range(1, round(1 / step))]
# в принципе можно, но лучше List Comprehension

quantiles = tuple(quantiles)

# Расчитываем квантили.
qvantiles_sql = f"""
    select qvan4
    from
    (
        select arrayPushFront(qvan3, min(rid_hash)) qvan4
             , length(qvan4) qty
             , quantiles{quantiles}(rid_hash) qvan
             , arrayMap(x -> toUInt64(x), qvan) qvan2
             , arrayPushBack(qvan2, max(rid_hash)) qvan3
        from {src_table}
    )
    """

quantile_result = client.execute(qvantiles_sql)[0][0]

iter_count = 0
for i in range(0, len(quantile_result)-1):

    rid_start = int(quantile_result[i])
    rid_end = int(quantile_result[i+1])

    iter_count += 1

    print(f"Итерация: {iter_count}. Обрабатываются заказы: {rid_start} - {rid_end}.")

    insert_query = f"""
        insert into {dst_table}
        with
        (
            select max(dt) from current.ridHello
        ) as dt_max_time
        select rid_hash
            , argMax(src_office_id, dt) src_office_id
            , argMax(dst_office_id, dt) dst_office_id
            , max(dt) dt_last
            , 0 is_deleted
        from {src_table}
        where rid_hash between {rid_start} and {rid_end}
        group by rid_hash
        having argMax(src, dt) = 'assembly_task'
            and dt_last < dt_max_time - interval 8 hour
        order by rid_hash
        """

    # Вставка данных
    client.execute(insert_query)


print(f"Таблица {dst_table} заполнена.")
