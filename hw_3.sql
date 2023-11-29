---=== Часть 1. Движок ReplacingMergeTree. ===---
-- Создать таблицу с такой структурой колонок. tmp.table3_3__ (использовать свой номер)
--     rid_hash      UInt64,
--     shk_id        Int64,
--     dt            DateTime,
--     src_office_id UInt32,
--     dst_office_id UInt32,
--     price_100     UInt32,
--     dt_load       DateTime,
--     src           String
-- Партиционирование не нужно.
-- Сортировка по номеру заказа.
-- Движок обычный MergeTree().
drop table if exists tmp.table3_310
create table tmp.table3_310
(
    rid_hash      UInt64,
    shk_id        Int64,
    dt            DateTime,
    src_office_id UInt32,
    dst_office_id UInt32,
    price_100     UInt32,
    dt_load       DateTime,
    src           String
)
engine = MergeTree()
order by rid_hash
settings index_granularity = 8192;

-- 01 Подготовить таблицу с тестовыми данными:
-- Из 6ти таблиц Хелло отобрать в вашу таблицу по 2 000 000 строк за 5 последних суток.
-- Должно получиться 12млн строк.
insert into tmp.table3_310
select rid_hash, shk_id, dt, src_office_id, dst_office_id, 0 price_100, dt_load, 'assembled' src
from history.assembled
where dt >= toStartOfDay(now()) - interval 5 day
limit 2000000
union all
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, 'assembly_task' src
from history.assembly_task
where dt >= toStartOfDay(now()) - interval 5 day
limit 2000000
union all
select rid_hash, shk_id, issued_dt dt, src_office_id, dst_office_id, price_100, dt_load, 'assembly_task_issued' src
from history.assembly_task_issued
where dt >= toStartOfDay(now()) - interval 5 day
limit 2000000
union all
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, 'sorted' src
from history.sorted
where dt >= toStartOfDay(now()) - interval 5 day
limit 2000000
union all
select rid_hash, 0 shk_id, dt, 0 src_office_id, 0 dst_office_id, 0 price_100, dt_load, 'order_completed' src
from history.order_completed
where dt >= toStartOfDay(now()) - interval 5 day
limit 2000000
union all
select rid_hash, 0 shk_id, reject_dt dt, src_office_id, dst_office_id, 0 price_100, dt_load, 'rejected' src
from history.rejected
where dt >= toStartOfDay(now()) - interval 5 day
limit 2000000

-- 02 Провести исследование отобранного набора данных.
-- Сколько уникальных заказов есть в тестовой выборке.
select uniq(rid_hash) qty_postions
from tmp.table3_310

-- 03 Сколько заказов было Оформлено, Отправлено на сборку, Собрано, Сортировано, Доставлено, Отменено.
-- Посчитать эти показатели колонками в одной строке.
select uniqIf(rid_hash, src = 'assembly_task') qty_assembly_task
    , uniqIf(rid_hash, src = 'assembly_task_issued') qty_assembly_task_issued
    , uniqIf(rid_hash, src = 'assembled') qty_assembled
    , uniqIf(rid_hash, src = 'sorted') qty_sorted
    , uniqIf(rid_hash, src = 'order_completed') qty_order_completed
    , uniqIf(rid_hash, src = 'rejected') qty_rejected
from tmp.table3_310

-- 04 Вывести 100 заказов, с наибольшей историей. Т.е. для которых было много событий в таблицах Хелло.
select rid_hash, count(src) qty
from tmp.table3_310
group by rid_hash
order by qty desc
limit 100

-- 05 Из предыдущего полученного результата выбрать один заказ с максимальным кол-вом истории.
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table3_310
where rid_hash = 163516703780423762
order by dt

-- 06 Сделать таблицу с движком ReplacingMergeTree. tmp.table4_3__
-- Структура таблицы такая же как у тестового набора данных.
-- Сортировка по rid_hash.
-- Партиционирование не нужно.
-- Залить в эту таблицу все данные из тестововой таблицы два раза.
-- Скорее всего двойная заливка сделает дубликаты заказов в таблице, которые движок не успеет удалить.
drop table if exists tmp.table4_310
create table tmp.table4_310
(
    rid_hash      UInt64,
    shk_id        Int64,
    dt            DateTime,
    src_office_id UInt32,
    dst_office_id UInt32,
    price_100     UInt32,
    dt_load       DateTime,
    src           String
)
engine = ReplacingMergeTree(dt)
order by rid_hash
settings index_granularity = 8192;

insert into tmp.table4_310
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table3_310

-- 07 Найти заказы, которые встречаются в таблице более 1го раза.
select rid_hash, count(rid_hash) qty
from tmp.table4_310
group by rid_hash
having qty > 1
order by qty

-- 08 Вывести один из таких заказов.
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table4_310
where rid_hash = 163516703780423762

-- 09 Применить ключевое слово final для удаления дублей из результата.
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table4_310 final
where rid_hash = 163516703780423762

-- 10 Вместо final применить функцию argMax, чтобы удалить дубли.
-- На данном шаге применить argMax к каждой колонке.
select rid_hash
    , argMax(shk_id, dt) shk_last
    , max(dt) dt_max
    , argMax(src_office_id, dt) src_office_last
    , argMax(dst_office_id, dt) dst_office_last
    , argMax(price_100, dt) price_100_last
    , argMax(dt_load, dt) dt_load_last
    , argMax(src, dt) src_last
from tmp.table4_310
where rid_hash = 163516703780423762
group by rid_hash

-- 11 Применить функцию argMax, чтобы удалить дубли.
-- На данном шаге применить argMax к Tuple.
-- Вывести все колонки.
select rid_hash
    , t_max.1 shk_last
    , t_max.2 dt_max
    , t_max.3 src_office_last
    , t_max.4 dst_office_last
    , t_max.5 price_100_last
    , t_max.6 dt_load_last
    , t_max.7 src_last
from
(
    select rid_hash
         , argMax((shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src), dt) t_max
    from tmp.table4_310
    where rid_hash = 163516703780423762
    group by rid_hash
)

-- 12 Сделать новую таблицу tmp.table5_3__ с сортировкой по rid.
-- Партиционирвоание по dt_date. Партиционирования по одномму дню.
-- Движок ReplacingMergeTree. Сортировка rid_hash.
-- Залить в нее все данные из тестового набора дынных один раз.
drop table if exists tmp.table5_310
create table tmp.table5_310
(
    dt_date       Date,
    rid_hash      UInt64,
    shk_id        Int64,
    dt            DateTime,
    src_office_id UInt32,
    dst_office_id UInt32,
    price_100     UInt32,
    dt_load       DateTime,
    src           String
)
engine = ReplacingMergeTree(dt)
order by rid_hash
partition by toYYYYMMDD(dt_date)
settings index_granularity = 8192;

insert into tmp.table5_310
select toDate(dt) dt_date, rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table3_310

-- 13 Вывести данные по выбранному заказу, который использовали в предыдущих запросах.
select dt_date, rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table5_310
where rid_hash = 163516703780423762

-- 14 Попытаться удалить дубли через команду optimize.
-- Почему дубли не удаляются. Написать словами.
optimize table tmp.table5_310 final
--При использовании движка ReplacingMergeTree вместе с партиционированием cliсkhouse отслеживает уникальность в каждой партиции

-- 15 Получить последнее состояние заказа. Применить функцию argMax().
select rid_hash
    , t_max.1 shk_last
    , t_max.2 dt_max
    , t_max.3 src_office_last
    , t_max.4 dst_office_last
    , t_max.5 price_100_last
    , t_max.6 dt_load_last
    , t_max.7 src_last
from
(
    select rid_hash
         , argMax((shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src), dt) t_max
    from tmp.table5_310
    where rid_hash = 163516703780423762
    group by rid_hash
)

---=== Часть 2. Движок Memory. ===---

-- 01 Создать таблицу с Memory(). Струкрута такая же как и в предыдущем задании.
create table temp_memory
(
    rid_hash      UInt64,
    shk_id        Int64,
    dt            DateTime,
    src_office_id UInt32,
    dst_office_id UInt32,
    price_100     UInt32,
    dt_load       DateTime,
    src           String
)
engine = Memory()

-- 02 Залить в нее данные из тестового набора. Сдедать выборку 100 любых строк.
insert into temp_memory
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table3_310
limit 100

-- 03 Удалить таблицу.
drop table temp_memory

---=== Часть 3. Временные таблицы. ===---

-- 01 Создать временную таблицу. Струкрута такая же как и в предыдущем задании.
create temporary table temp_table
(
    rid_hash      UInt64,
    shk_id        Int64,
    dt            DateTime,
    src_office_id UInt32,
    dst_office_id UInt32,
    price_100     UInt32,
    dt_load       DateTime,
    src           String
)

-- 02 Залить в нее данные из тестового набора. Сдедать выборку 100 любых строк.
insert into temp_table
select rid_hash, shk_id, dt, src_office_id, dst_office_id, price_100, dt_load, src
from tmp.table3_310
limit 100

-- 03 Удалить таблицу.
drop table temp_table
