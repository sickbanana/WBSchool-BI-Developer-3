drop table if exists tmp.table11_1_310;
SET max_execution_time = 15000000
create table tmp.table11_1_310 ENGINE = MergeTree order by (shk_id) as
select item_id shk_id, dt dt_ocr
from history.ShkOnPlace
where dt >= toStartOfDay(now()) - interval 30 day
    and state_id = 'OCR'

drop table if exists tmp.table11_2_310;
SET max_execution_time = 15000000
create table tmp.table11_2_310 ENGINE = MergeTree order by (rid_hash) as
select rid_hash, shk_id, min(dt) dt_assembly
from history.assembled
where dt >= toStartOfDay(now()) - interval 30 day
    and shk_id in (select shk_id from tmp.table11_1_310)
group by rid_hash, shk_id

drop table if exists tmp.table11_3_310;
SET max_execution_time = 15000000
create table tmp.table11_3_310 ENGINE = MergeTree order by (rid_hash) as
select rid_hash, dt_ocr, shk_id
from tmp.table11_2_310 t2
asof join tmp.table11_1_310 t1
on t1.shk_id = t2.shk_id and t1.dt_ocr > t2.dt_assembly

drop table if exists tmp.table11_4_310;
SET max_execution_time = 15000000
create table tmp.table11_4_310 ENGINE = MergeTree order by (shk_id) as
select item_id shk_id, dt dt_mx, mx, state_id, employee_id
    , dictGet('dictionary.StoragePlace','office_id', toUInt64(mx)) office_id
from history.ShkOnPlace
where dt >= now() - interval 30 day
    and item_id in (select shk_id from tmp.table11_3_310)
    and (state_id = 'WPU' or dictGet('dictionary.BranchOffice', 'type_point', toUInt64(office_id)) = 13);

drop table if exists tmp.table11_5_310;
SET max_execution_time = 15000000
create table tmp.table11_5_310 ENGINE = MergeTree order by (rid_hash) as
select rid_hash, shk_id, dt_ocr, dt_mx, mx, state_id, employee_id
from tmp.table11_4_310 t4
asof join tmp.table11_3_310 t3
on t4.shk_id = t3.shk_id and t4.dt_mx > t3.dt_ocr


-- 01
-- Добавить в выгрузку колонки:
-- src_office_id - Офис оформления заказа.
-- src_office_name
--   Для получения src_office_id нужно дописать код во времянке tmp.table11_6.
drop table if exists tmp.table11_6_310;
SET max_execution_time = 15000000
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

-- dt_return - Дата когда ШК попал на МХ офиса оформления после возврата.
-- mx_return - первое МХ офиса оформления, после возврата
-- mx_return_name
--   Эти колонки нужно получить в Результирующем запросе..

-- Поправьте условие в конечном запросе, чтобы выводились заказы с Переупаковкой или Вернувшиеся в офис оформления.
--   Сейчас запрос показывает только Переупаковку.

select rid_hash
     , shk_id
     , dt_ocr
     , src_office_id
     , dictGet('dictionary.BranchOffice', 'office_name', src_office_id) src_office_name
     , minIf(dt_mx, office_id = src_office_id) dt_return
     , argMinIf(mx, dt_mx, office_id = src_office_id) mx_return
     , dictGet('dictionary.StoragePlace','mx_name', mx_return) mx_return_name
     , minIf(dt_mx, state_id = 'WPU') dt_repack
     , argMinIf(mx, dt_mx, state_id = 'WPU') mx_repack
     , dictGet('dictionary.StoragePlace','mx_name', mx_repack) mx_repack_name
     , argMinIf(employee_id, dt_mx, state_id = 'WPU') emp_repack
from tmp.table11_6_310
group by rid_hash, shk_id, dt_ocr, src_office_id
having dt_repack != 0 or dt_return != 0
limit 100


-- 02
-- Какие статусы бывают у возвращаемых заказов до того как они вернутся в офис или попадут на переупаковку.
-- Сделайте запрос-1 к одной из времянок, чтобы получить список таких статусов.
select rid_hash, shk_id, dt_mx, mx, state_id
    , dictGet('dictionary.State', 'state_descr', state_id) state_descr
from tmp.table11_6_310
order by shk_id, dt_mx

-- Сделайте запрос-2, который показывает детализацию по одному ШК, где видно Заказ, ШК, Дата, МХ, Статус.
-- Желательно подобрать ШК с большим кол-вом статусов.
select rid_hash, shk_id, dt_mx, mx, state_id
from tmp.table11_6_310
where shk_id = 16480094243


-- 03
-- Для одного ШК, который вернулся на склад оформления и прошел переупаковку показать детализацию с колонками:
-- Заказ, ШК, Дата, МХ, Статус, Офис, Тип офиса(type_point из словаря dictionary.BranchOffice)
-- Желательно подобрать ШК с большим кол-вом МХ.
select rid_hash, shk_id, dt_mx, mx, state_id, office_id
    , dictGet('dictionary.BranchOffice', 'type_point', toUInt64(office_id)) type_point
from tmp.table11_6_310
where shk_id = 16480094243
order by shk_id, dt_mx