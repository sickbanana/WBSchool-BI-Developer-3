drop table if exists tmp.table9_310_1;
SET max_execution_time = 15000000
create table tmp.table9_310_1 ENGINE = MergeTree order by (rid_hash) as
with
(
    select max(dt) from current.ridHello
) as dt_max
select rid_hash
    , dt dt_last
    , shk_id
from current.ridHello final
where dt >= toStartOfDay(now()) - interval 5 day
    and src = 'assembled'
    and dateDiff('hour', dt_last, dt_max) >= 8


drop table if exists tmp.table9_310_2;
SET max_execution_time = 15000000
create table tmp.table9_310_2 ENGINE = MergeTree order by (shk_id) as
select item_id        shk_id
     , argMax(mx, dt) mx_last
     , max(dt)        dt_mx_last
     , argMaxIf(state_id, dt, lengthUTF8(state_id) > 0) state_last
     , argMax(employee_id, dt)                          emp_last
from current.ShkOnPlace
where dt >= toStartOfDay(now()) - interval 6 day
    and item_id in (select shk_id from tmp.table9_310_1)
group by item_id

-- 01. Добавить колонку src_office_id.
-- В таблице assembled колонка src_office_id везде равна 0.
-- Поэтому нужно сходить в предыдущую таблицу и взять оттуда последний src_office_id для заказа.
-- Собрать времянку tmp.table9_3 и добавить join в конечный запрос.
-- Не для всех заказов найдется src_office_id. Поэтому в конечном запросе выберите нужный вид join, чтобы отсечь заказы без src_office_id.
-- Глубина даты 10 дней, этого должно хватить для поиска.
drop table if exists tmp.table9_310_3;
SET max_execution_time = 15000000
create table tmp.table9_310_3 ENGINE = MergeTree order by (rid_hash) as
select rid_hash
    , argMax(src_office_id, issued_dt) src_office_id
    , argMax(nm_id, issued_dt) nm_id
from history.assembly_task_issued
where issued_dt >= toStartOfDay(now()) - interval 10 day
    and rid_hash in (select rid_hash from tmp.table9_310_1)
group by rid_hash

-- 02. Добавить колонку wbsticker_id.
-- Собрать времянку tmp.table9_4 и добавить join в конечный запрос.
-- Глубина даты 10 дней, этого должно хватить для поиска.
    
    -- в фильтрации лучше работать с item_id, а не с преобразованным shk_id.
    -- в таблице WBSticker сортировка идёт по item_id
drop table if exists tmp.table9_310_4;
SET max_execution_time = 15000000
create table tmp.table9_310_4 ENGINE = MergeTree order by (shk_id) as
select toInt64(item_id) shk_id
    , argMax(wbsticker_id, dt) wbsticker_id
from history.WBSticker
where dt >= toStartOfDay(now()) - interval 10 day
    and item_id in (select toUInt64(shk_id) from tmp.table9_310_1)
group by shk_id

-- 03. Добавить колонку объем в литрах. Округлить до 2х знаков.
-- Использовать витрину с объемами.
-- Объемы можно получить по идентификатору номенклатуры nm_id.
-- Колонку nm_id можно получить вместе с src_office_id в решении задачи Задачи-1.
-- Собрать времянку tmp.table9_5 и добавить join в конечный запрос.
drop table if exists tmp.table9_310_5;
SET max_execution_time = 15000000
create table tmp.table9_310_5 ENGINE = MergeTree order by (nm_id) as
select nm_id
    , round(vol / 1000, 2) vol_l
from report.volume_by_nm final
where nm_id in (select toUInt64(nm_id) from tmp.table9_310_3)

    -- не надо inner join, достаточно найти первое совпадение и выйти из join секции
drop table if exists tmp.table9_310_main;
SET max_execution_time = 15000000
create table tmp.table9_310_main ENGINE = MergeTree order by (rid_hash) as
select t1.rid_hash rid_hash
    , dt_last
    , t3.src_office_id src_office_id
    , t1.shk_id shk_id
    , wbsticker_id
    , dictGet('dictionary.StoragePlace','office_id', mx_last) office_id_current
    , mx_last
    , dt_mx_last
    , state_last
    , emp_last
    , vol_l
    , now() dt_last_load
from tmp.table9_310_1 t1
left any join tmp.table9_310_2 t2
    on t1.shk_id = t2.shk_id
semi join tmp.table9_310_3 t3
    on t1.rid_hash = t3.rid_hash
left any join tmp.table9_310_4 t4
    on t1.shk_id = t4.shk_id
left any join tmp.table9_310_5 t5
    on t3.nm_id = t5.nm_id







select count()
from tmp.table9_310_main
--16654130
select count()
from tmp.table9_310_3
--16654130
