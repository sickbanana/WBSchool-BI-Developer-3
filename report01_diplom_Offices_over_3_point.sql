-- 01 Схема Витрины
shippingroute_name, [offices_name_points] arr_points, qty_points x 1, rid_hash, src_office_id, shk_id, dt_start, dt_finish x 50

drop table if exists offices_over_3_points_310
create table offices_over_3_points_310
(
    `src_office_id` DateTime,
    `shippingroute_name` String,
    `arr_point` Array(String),
    `qty_points` Int32,
    `rid_hash` UInt64,
    `shk_id` Int64,
    `dt_start` DateTime,
    `dt_finish` DateTime,
    `dt_load` DateTime
)
engine = MergeTree()
order by (src_office_id, shippingroute_name)
ttl toStartOfDay(dt_load) + toIntervalDay(5)
settings index_granularity = 8192

-- 02 Текст с ответами.
-- а) что планируете хранить в витрине. в каком разрезе.
офис оформления, маршрут, данные для детализации для каждого маршрута - номер заказа, шк, дата оформления, дата доставки
-- б) сколько по времени будет храниться инфа в вашей витрине, как будет она будет удаляться.
Данные будут храниться 5 дней, удаляться будет с помощью ttl
-- в) какой движок планируете использовать и почему.
MergeTree так как у нас не должно быть дубликатов
-- г) какая сортировка и почему.
сортировка по названию маршрута и офису оформления, мы будем делать группировку по этим полям
-- д) как в даге будете обновлять уже имеющуюся в витрине инфу и что конкретно обновлять.
в витрине нас интересуют уже доставленные заказы, поэтому при обновлении будем только добавлять новые с помощью времянок


-- 03 Скрипты сборки витрины
-- Отчет: Доставленные заказы с кол-вом офисов более 3х точек.
-- Отчет должен показывать фактические офисы, которые проехал товар от статуса заказа Оформлен до Доставлен.
-- Показывать только те маршруты, у которых было более 3х офисов в пути. ПВЗ исключаем (dst_office_id).
-- Отчет должен показывать информацию для Выполненых заказов за последние 5 дней.
-- Обновление 1 раз в день.
-- После проверки куратором, заполнить витрину через квантили в 10 итераций по rid_hash.
drop table if exists tmp.table_01_diplom_1_310
SET max_execution_time = 1500000
create table tmp.table_01_diplom_1_310 engine MergeTree() order by (rid_hash) as
select rid_hash, max(dt) dt_finish
from history.order_completed
where dt >= now() - interval 5 day
group by rid_hash
limit 10000