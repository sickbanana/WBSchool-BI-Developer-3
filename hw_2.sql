-- ДЗ. Код по каждому шагу выложить в свой гит в едином файле.

drop table if exists tmp.table310
-- 01 Создать таблицу со своим номером tmp.table30_ (Например, tmp.table376)
-- Движок MergeTree.
-- Партиционирование:
--      для номеров 301-312 по 1 дню
--      для номеров 313-321 по 1 неделе
-- Сортировка:
--      для номеров 301-312 по shk_id
--      для номеров 313-321 по src_office_id
-- гранулярность индекса 8192
-- Колонки:
--     `rid_hash`
--     `dt`
--     `shk_id`
--     `src_office_id`
--     `dst_office_id`
create table tmp.table310
(
    `rid_hash` UInt64,
    `dt` DateTime,
    `shk_id` Int64,
    `src_office_id` UInt32,
    `dst_office_id` UInt32
)
engine = MergeTree
partition by toYYYYMMDD(dt)
order by shk_id
settings index_granularity = 8192

-- 02 Добавить материализованную колонку dt_date с типом Date, которая будет считать дату от колонки dt.
-- Про материализованные колонки посмотреть в документации.
alter table tmp.table310 add column dt_date Date materialized toDate(dt)

-- 03 Добавить материализованную колонку dt_last_load, которая будет заполняться текущем временем на момент вставки данных.
alter table tmp.table310 add column dt_last_load DateTime materialized now()

-- 02 Вставить в таблицу данные, чтобы получилось 10 партиций.
insert into tmp.table310 (rid_hash, dt, shk_id, src_office_id, dst_office_id)
values (9999,now()-interval 1 day,9999,9999,9999),
(9999,now()-interval 2 day,9999,9999,9999),
(9999,now()-interval 3 day,9999,9999,9999),
(9999,now()-interval 4 day,9999,9999,9999),
(9999,now()-interval 5 day,9999,9999,9999),
(9999,now()-interval 6 day,9999,9999,9999),
(9999,now()-interval 7 day,9999,9999,9999),
(9999,now()-interval 8 day,9999,9999,9999),
(9999,now()-interval 9 day,9999,9999,9999),
(9999,now()-interval 10 day,9999,9999,9999)

-- Приложить запрос просмотра системной информации о вашей таблице.
select partition, name part, min_time, max_time, active, marks, rows
    , round(bytes_on_disk/1024/1024,2) Mb
    , engine
from system.parts
where database = 'tmp'
    and table = 'table310'
order by partition, name

-- 03 Удалить 3 последние партиции.
alter table tmp.table310 drop partition 20231124,
    drop partition 20231123,
    drop partition 20231122

-- 04 Удалить все данные в крайней старшей партиции через мутацию.
alter table tmp.table310 delete where toYYYYMMDD(dt) = 20231121

-- 05 Добавить колонку column10 в конец таблицы.
alter table tmp.table310 add column column10 Int64 after dst_office_id

-- 06 Добавить колонку column1 в начало таблицы.
alter table tmp.table310 add column column1 Int64 first

-- 07 Добавить колонку с типом
--      для номеров 301-312 Массив положительных чисел
--      для номеров 313-321 Массив строк
alter table tmp.table310 add column array_of_positive Array(UInt64)

-- 08 Вставить 3 новые строки с 3мя элементами массива.
insert into tmp.table310 (rid_hash, dt, shk_id, src_office_id, dst_office_id, array_of_positive)
values (9999,now()-interval 1 day,9999,9999,9999, [1, 2, 3]),
(9999,now()-interval 1 day,9999,9999,9999, [2, 3, 4]),
(9999,now()-interval 1 day,9999,9999,9999, [3, 4, 5])

-- 09 Добавить колонку с типом
--      для номеров 301-312 Массив последовательности (DateTime, UInt64)
--      для номеров 313-321 Массив последовательности (DateTime, Date).
alter table tmp.table310 add column array_of_tuple Array(Tuple(DateTime, UInt64))

-- 10 Вставить 3 новые строки с 3мя элементами массива.
insert into tmp.table310 (rid_hash, dt, shk_id, src_office_id, dst_office_id, array_of_positive, array_of_tuple)
values (9999,now()-interval 1 day,9999,9999,9999, [1, 2, 3], [(now(), 1), (now(), 2), (now(), 3)]),
(9999,now()-interval 1 day,9999,9999,9999, [2, 3, 4], [(now(), 10), (now(), 2), (now(), 4)]),
(9999,now()-interval 1 day,9999,9999,9999, [3, 4, 5], [(now(), 13), (now(), 2), (now(), 5)])

-- 11 Добавить материализованную колонку массив, чтобы она заполнялась из колонок dt, rid_hash.
alter table tmp.table310 add column mat_array Tuple(DateTime, UInt64) materialized (dt, rid_hash)

-- 12 Вставить 3 новые строки.
insert into tmp.table310 (rid_hash, dt, shk_id, src_office_id, dst_office_id, array_of_positive, array_of_tuple)
values (9990,now()-interval 2 day,9990,9990,9990, [1, 2, 3], [(now(), 10), (now(), 20), (now(), 30)]),
(9990,now()-interval 2 day,9990,9990,9990, [2, 3, 4], [(now(), 11), (now(), 21), (now(), 40)]),
(9990,now()-interval 2 day,9990,9990,9990, [3, 4, 5], [(now(), 12), (now(), 22), (now(), 50)])

-- 13 Удалить колонку dst_office_id.
alter table tmp.table310 drop column if exists dst_office_id

drop table if exists tmp.table2_310
-- 14 Создать еще одну таблицу tmp.table2_3__ со структурой, которую мы получили в предыдущих шагах.
-- При создании таблицы сделать TTL:
--      для номеров 301-312 1 день
--      для номеров 313-321 1 неделя
create table tmp.table2_310
(
    `column1` Int64,
    `rid_hash` UInt64,
    `dt` DateTime,
    `shk_id` Int64,
    `src_office_id` UInt32,
    `column10` Int64,
    `array_of_positive` Array(UInt64),
    `array_of_tuple` Array(Tuple(DateTime, UInt64)),
    `dt_date` Date materialized toDate(dt),
    `dt_last_load` DateTime materialized now(),
    `mat_array` Tuple(DateTime, UInt64) materialized (dt, rid_hash)

)
engine = MergeTree
partition by toYYYYMMDD(dt)
order by shk_id
ttl toStartOfDay(dt) + interval 1 day
settings index_granularity = 8192

-- 15 Залить данные из первой таблицы во вторую.
insert into tmp.table2_310
select *
from tmp.table310

optimize table tmp.table2_310 final
-- 16 Добавить код запроса просмотра системной информации своей таблицы.
select partition, name part, min_time, max_time, active, marks, rows
    , round(bytes_on_disk/1024/1024,2) Mb
    , engine
from system.parts
where database = 'tmp'
    and table = 'table2_310'
order by partition, name

-- В итоге у каждого должны получиться 2 таблицы в схеме tmp.)


--=== Задание-2 ===--

-- Порядок колонок использовать как написано в задании. Это сокращает время проверки)
-- За последние 3е суток по офису Хабаровск id 2400 вывести 100 заказов со следующими колонками:
-- 1. rid_hash
-- 2. Дата последнего статуса Отправлен на сборку. Колонку округлить до ближайших 15 минут. См.соответсвующую функцию.
-- 3. Дата первого статуса Сортирован.
-- 4. Разница в Часах между 2мя колонками с датами.
-- 5. Посчитать кол-во уникальных ШК в заказе. Бывает ШК меняется в заказе.
-- 6. Вывести все ШК в массив. Использовать соответствующую функцию при группировке. Массив должен быть без дублей и отсортирован по возрастанию.
--    Также элементы массива должны быть не четными)
-- 7. Вывести последний элемент массива.
-- 8. Текстовая колонка: "Последний элемент массива: 4783689345"
--* join не используем)

-- Почему у некоторых заказов diff_h принимает аномально высокие или низкие значения. Написать словами почему так произошло.
-- Какое условие в запрос можно добавить, чтобы это избежать.


--=== Задание-3 ===--
-- Для офисов, у которых за 3 дня было между 10т и 50т заказов в статусе Оформлен, вывести следующую информацию.
-- За 3 дня показать 5 заказов по этим офисам за каждый день, которые были в статусе Сортирован.
-- Для вывода 5 заказов использовать оператор limit 5 by ...
-- Колонки: src_office_id, office_name, dt_date, rid_hash, shk_id.
-- * join не используем)

select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , toDate(dt) dt_date
    , rid_hash
    , shk_id
from history.sorted
where dt >= toStartOfDay(now()) - interval 3 day
    and src_office_id in
    (
        select src_office_id
        from history.assembly_task
        where dt >= toStartOfDay(now()) - interval 3 day
        group by src_office_id
        having uniq(rid_hash) between 10000 and 50000
    )
order by dt
limit 5 by src_office_id, dt_date


--=== Задание-4 ===--
-- Для офисов, у которых за 3 дня процент заказов Оформлен к Сортирован в диапазоне 30-50% вывести следующую информацию.
-- За 3 дня показать 5 заказов по каждому офису за каждый день по статусу из Отмена.
-- Для вывода 5 заказов использовать оператор limit 5 by ...
-- Колонки: src_office_id, office_name, dt_date, rid_hash, nm_id, sm_id.
-- * join не используем)

select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , toDate(reject_dt) dt_date
    , rid_hash
    , nm_id
    , sm_id
from history.rejected
where dt_date >= toStartOfDay(now()) - interval 3 day
    and src_office_id in
    (
        select src_office_id
        from history.assembly_task
        where dt >= toStartOfDay(now()) - interval 3 day
        group by src_office_id
        having uniq(rid_hash) / (uniq(rid_hash))  between 0.3 and 0.5
    )
limit 5 by src_office_id, dt_date

# limit чаще всего используется с еще одним операндом, у тебя его не вижу
