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
where dt >= (select max(dt_last) from report.orders_not_in_assembly_310 where is_deleted = 0) - interval 10 hour
    and rid_hash in (select rid_hash from report.orders_not_in_assembly_310 where is_deleted = 0)
group by rid_hash
having argMax(src, dt) != 'assembly_task'
    or dt_last > dt_max_time - interval 8 hour
order by rid_hash
limit 10
# orders_not_in_assembly_310 - какой движек?

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
where dt >= (select max(dt_last) from report.orders_not_in_assembly where is_deleted = 0) - interval 10 hour
    and rid_hash not in (select rid_hash from report.orders_not_in_assembly_310 where is_deleted = 0)
group by rid_hash
having argMax(src, dt) = 'assembly_task'
    and dt_last < dt_max_time - interval 8 hour
