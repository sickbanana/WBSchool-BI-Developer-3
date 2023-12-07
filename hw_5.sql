-- 01
-- Создать витрину со своим номером.
-- Заполнить витрину за весь период по всем офисам.
-- Скрипт python.
drop table if exists report.orders_not_in_assembly_310
create table report.orders_not_in_assembly_310
(
    rid_hash      UInt64,
    src_office_id UInt32,
    dst_office_id UInt32,
    dt_last       DateTime,
    is_deleted    UInt8
)
    engine = ReplacingMergeTree(dt_last)
    order by rid_hash
    ttl dt_last + interval 1 second where is_deleted = 1
    settings index_granularity = 8192

-- 02
-- Дописать запрос.
-- Пометить заказы удаленными, которые сменили Статус или у которых стало менее 8ч в статусе Оформлен.
-- Инкрементально. Т.е. начиная с даты последней проверки.
with
(
    select max(dt) from current.ridHello
) as dt_max_time
select rid_hash
    , argMax(src_office_id, dt) src_office_id
    , argMax(dst_office_id, dt) dst_office_id
    , max(dt) dt_last
    , 1 is_deleted
from current.ridHello
where dt >= (select max(dt_last) from report.orders_not_in_assembly_310) - interval 10 hour
    and rid_hash in (select rid_hash from report.orders_not_in_assembly_310 final where is_deleted = 0)
group by rid_hash
having argMax(src, dt) != 'assembly_task'
    or dt_last > dt_max_time - interval 8 hour
order by rid_hash
# orders_not_in_assembly_310 - какой движек?
# dt >= (select max(dt_last) from report.orders_not_in_assembly_310 where is_deleted = 0) если у тебя условие на is_deleted, то нужен final
# но чет похоже оно тут лишнее - where is_deleted = 0

-- 03
-- Дописать запрос.
-- Добавить новые заказы, у которых есть отставание. Но которых нет в витрине.
-- Инкрементально.
with
(
    select max(dt) from current.ridHello
) as dt_max_time
select rid_hash
    , argMax(src_office_id, dt) src_office_id
    , argMax(dst_office_id, dt) dst_office_id
    , max(dt) dt_last
    , 0 is_deleted
from current.ridHello
where dt >= (select max(dt_last) from report.orders_not_in_assembly_310 where is_deleted = 0) - interval 10 hour
    and rid_hash not in (select rid_hash from report.orders_not_in_assembly_310 final where is_deleted = 0)
group by rid_hash
having argMax(src, dt) = 'assembly_task'
    and dt_last < dt_max_time - interval 8 hour

-- 05
-- Написать запрос к витрине.
-- Кол-во заказов по каждому офису оформления и за каждый день.
-- Добавить колонку Имя офиса. Упорядочить по офису и по дате.
select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , toDate(dt_last) dt_date
    , count(rid_hash) qty
from report.orders_not_in_assembly_310 final
where is_deleted = 0
group by src_office_id, dt_date
order by src_office_id, dt_date
# a тут не надо на is_delete условие?
# и final?
