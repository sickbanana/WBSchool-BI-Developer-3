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

# если в оригинальной таблице будет заптси от октября, то в твою витрину они не попадут 
# я немного не понял, у нашей же витрины ttl 30 дней
# это да, но данные у тебя в таблице не за все 30 дней сейчас есть, а твой код за них тоже отработает, но ничего не найдёт
# лучше будет узнать разницу между максимальной и минимальной датой в изначальной таблице

# кстати, за сегодняшний день у тебя запрос не отработоет
diff_day_query = f""" 
    select date_diff('day', min(dt), max(dt))
    from {src_table}
    """

diff_day = client.execute(diff_day_query)[0][0]

query_len = 0

for i in range(0, diff_day + 1):


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
        where dt >= today() - interval {i} day and dt < today() - interval {i - 1} day
        -- у тебя ещё будут затрагиваться операции из первого часа следующего дня
        -- лучше перепиши через уровнения 

        -- перепиши через уроовнения dt >= и dt <
        -- now() - interval 1 day это не за день, а за последние 24 часа, есть другая функция для дня

        -- норм, но лучше используй что-то из этого набора: select toStartOfDay(now()), toDate(now()), today()
        -- и кстати, сегодняшний день у тебя не предусмотрен
        
        -- предусмотрен у меня на первой итерации dt меньше завтрашней даты должно быть и больше или равно 00:00 сегодняшний
        --, в данный момент в calc нет данных за сегодня
        
        group by prodtype_id, dt_h, dt_h_msk, employee_id, office_id, calc_date
        """

    data = client.execute(select_query)

    client.execute(f"insert into {dst_table} values", data)

    query_len += len(data)

print(f"Добавлено {query_len} данных")
print(f"Таблица заполнена.")
