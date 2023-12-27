-- Сделать выгрузку для брака.
-- Выгрузка - файл эксель или гугл-док с данными.
-- Есть статусы попадания ШК на Брак.
-- Брак - это департамент, который проверяет вещи на предмет брака.
-- За последний месяц показать вешь попавшую на брак по статусам ('SDL', 'SDO', 'UDG', 'SDW', 'SDU').
-- Для каждого факт попадания на брак показать 3 предыдущих состояния по МХ: статус, сотрудник, дата.
-- Колонки:
--  office_id        - Офис где было событие по браку
--  office_name      - Офис где было событие по браку
--  shk_id           - ШК попавший на брак
--  dt_brak          - дата события по браку
--  emp_brak         - сотрудник по браку
--  state_brak       - статус брак
--  state_brak_descr - статус брак
--  dt_prev          - дата предыдущего события
--  state_prev       - статус предыдущего события
--  state_prev_descr - имя статуса предыдущего события
--  emp_prev         - сотрудник предыдущего события


drop table if exists tmp.table10_1_310;
SET max_execution_time = 15000000
create table tmp.table10_1_310 ENGINE = MergeTree order by (shk_id) as
select item_id shk_id, state_id state_brak, employee_id emp_brak, dt dt_brak
     , dictGet('dictionary.StoragePlace','office_id', mx) office_id
from history.ShkOnPlace
where dt >= now() - interval 1 month
    and state_id in ('SDL', 'SDO', 'UDG', 'SDW', 'SDU')

drop table if exists tmp.table10_2_310;
SET max_execution_time = 15000000
create table tmp.table10_2_310 ENGINE = MergeTree order by (shk_id) as
select item_id shk_id, state_id, employee_id, dt
from history.ShkOnPlace
where dt >= now() - interval 2 month
    and item_id in (select shk_id from tmp.table10_1_310)
    and state_id not in ('SDL', 'SDO', 'UDG', 'SDW', 'SDU')

drop table if exists tmp.table10_3_310;
SET max_execution_time = 15000000
create table tmp.table10_3_310 ENGINE = MergeTree order by (shk_id) as
select shk_id, dt_brak, emp_brak, state_brak, dt dt_prev, state_id state_prev, employee_id emp_prev, office_id
from tmp.table10_2_310 t2
asof join tmp.table10_1_310 t1
on t2.shk_id = t1.shk_id and t2.dt < t1.dt_brak
order by shk_id, dt desc
limit 3 by shk_id, dt_brak

select count() qty
from tmp.table10_3;

-- ДЗ.
-- Добавить колонки.
-- Заказ, который был перед попаданием на Брак. Если нет заказа - не критично, показываем основную инфу.
--  rid_prev, dt_rid_prev
-- Заказ, который был после попаданием на Брак. Если нет заказа - не критично, показываем основную инфу.
--  rid_next, dt_rid_next
-- Следущее МХ после попадания на Брак. Если нет след МХ - не критично, показываем основную инфу.
--  dt_mx_next, mx_name_next

-- 01. Номер заказа, который предшествовал попаданию на брак.
drop table if exists tmp.table10_4_310;
SET max_execution_time = 15000000
create table tmp.table10_4_310 ENGINE = MergeTree order by (shk_id) as
select shk_id, rid_hash rid_prev
    , max(dt) dt_rid_prev
from history.sorted
where dt >= now() - interval 2 month
      and shk_id in (select shk_id from tmp.table10_1_310)
group by shk_id, rid_hash

-- 02. Номер заказа, который был после брака.
drop table if exists tmp.table10_5_310;
SET max_execution_time = 15000000
create table tmp.table10_5_310 ENGINE = MergeTree order by (shk_id) as
select shk_id, rid_hash rid_next
    , max(issued_dt) dt_rid_next
from history.assembly_task_issued
where issued_dt >= now() - interval 1 month
      and shk_id in (select shk_id from tmp.table10_1_310)
group by shk_id, rid_hash

-- 03. Следующее событие после брака.
-- Добавить колонку мх во времянку tmp.table10_2 и добавить эту времянку в результирующий запрос.
drop table if exists tmp.table10_2_310;
SET max_execution_time = 15000000
create table tmp.table10_2_310 ENGINE = MergeTree order by (shk_id) as
select item_id shk_id, state_id, employee_id, dt, mx
from history.ShkOnPlace
where dt >= now() - interval 2 month
    and item_id in (select shk_id from tmp.table10_1_310)
    and state_id not in ('SDL', 'SDO', 'UDG', 'SDW', 'SDU')

-- 04. Результирущая времянка
drop table if exists tmp.table10_main_310;
SET max_execution_time = 15000000
create table tmp.table10_main_310 ENGINE = MergeTree order by (shk_id) as
select office_id, t3.shk_id shk_id, dt_brak, emp_brak, state_brak, dt_prev, state_prev, emp_prev, rid_prev, dt_rid_prev
    , rid_next, dt_rid_next, mx_name_next, dt_mx_next
from tmp.table10_3_310 t3
asof left join
tmp.table10_4_310 t4
on t4.shk_id = t3.shk_id and t4.dt_rid_prev < t3.dt_brak
asof left join
tmp.table10_5_310 t5
on t5.shk_id = t3.shk_id and t5.dt_rid_next > t3.dt_brak
asof left join
(
    select shk_id, dt dt_mx_next, mx mx_name_next
    from tmp.table10_2_310
) t2
on t2.shk_id = t3.shk_id and t2.dt_mx_next > t3.dt_brak

select count()
from tmp.table10_3_310
--2059132

select count() qty
from tmp.table10_main_310
--2059132