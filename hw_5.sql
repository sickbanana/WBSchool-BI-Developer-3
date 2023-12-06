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