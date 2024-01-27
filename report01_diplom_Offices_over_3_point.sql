-- 01 Схема Витрины
shippingroute_name, [offices_name_points] arr_points, qty_points x 1, rid_hash, src_office_id, shk_id, dt_start, dt_finish x 50

drop table if exists report.offices_over_3_points_310
create table report.offices_over_3_points_310
(
    `src_office_id` UInt32,
    `shippingroute_name` String,
    `arr_point` Array(String),
    `qty_rid` Int32,
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

drop table if exists tmp.table_01_diplom_2_310
SET max_execution_time = 1500000
create table tmp.table_01_diplom_2_310 engine MergeTree() order by (rid_hash) as
select rid_hash, shk_id, max(dt) dt_start
from history.assembled
where dt >= now() - interval 30 day
    and rid_hash in (select rid_hash from tmp.table_01_diplom_1_310)
group by rid_hash, shk_id

select count()
from tmp.table_01_diplom_2_310

drop table if exists tmp.table_01_diplom_3_310
SET max_execution_time = 1500000
create table tmp.table_01_diplom_3_310 engine MergeTree() order by (rid_hash) as
select rid_hash, src_office_id, dst_office_id
    , dictGet('dictionary.OutfitAssemblySettings','shippingroute_id', (toUInt64(src_office_id), toUInt64(dst_office_id))) shippingroute_id
from history.assembly_task_issued
where issued_dt >= now() - interval 30 day
    and rid_hash in (select rid_hash from tmp.table_01_diplom_2_310)

select count()
from tmp.table_01_diplom_3_310

drop table if exists tmp.table_01_diplom_4_310
SET max_execution_time = 1500000
create table tmp.table_01_diplom_4_310 engine MergeTree() order by (shk_id) as
select t1.rid_hash rid_hash, src_office_id, dst_office_id, shk_id, dt_start, dt_finish
     , dictGet('dictionary.ShippingRoute','shippingroute_name', shippingroute_id) shippingroute_name
from tmp.table_01_diplom_1_310 t1
left join tmp.table_01_diplom_2_310 t2
on t1.rid_hash = t2.rid_hash
semi join tmp.table_01_diplom_3_310 t3
on t1.rid_hash = t3.rid_hash

select count()
from tmp.table_01_diplom_4_310

drop table if exists tmp.table_01_diplom_5_310
SET max_execution_time = 1500000
create table tmp.table_01_diplom_5_310 engine MergeTree() order by (shk_id) as
select item_id shk_id, dt, mx
    , dictGet('dictionary.StoragePlace','office_id', toUInt64(mx)) office_id
from history.ShkOnPlace
where shk_id in (select shk_id from tmp.table_01_diplom_4_310)

select count()
from tmp.table_01_diplom_5_310

drop table if exists tmp.table_01_diplom_6_310
SET max_execution_time = 1500000
create table tmp.table_01_diplom_6_310 engine MergeTree() order by (rid_hash) as
select shippingroute_name, rid_hash, t5.shk_id shk_id, src_office_id, dst_office_id, dt_start, dt_finish, office_id, dt
from tmp.table_01_diplom_5_310 t5
asof join tmp.table_01_diplom_4_310 t4
on t5.shk_id = t4.shk_id and dt > dt_start
where dt < dt_finish
    and office_id != src_office_id or office_id != dst_office_id

select count()
from tmp.table_01_diplom_6_310;

drop table if exists tmp.table_01_diplom_main_310
SET max_execution_time = 1500000
create table tmp.table_01_diplom_main_310 engine MergeTree() order by (src_office_id, shippingroute_name) as
select shippingroute_name, arr4 arr_points, rid_hash, shk_id, src_office_id, dt_start, dt_finish
    , arraySort(groupArray((dt, office_id))) arr1
    , arrayFilter(x -> x.2 != 0, arr1) arr2 -- из-за нулевых офиссов после их удаления могут быть повторения, поэтому удаляю предварительно тоже, есть вариант присваивать не 0 а -1 тогда в функции ниже
    , arrayMap(x -> (if(arr2[x].2 != arr2[x+1].2, arr2[x].2, 0)), arrayEnumerate(arr1)) arr3
    , arrayFilter(x -> x != 0, arr3) arr4
from tmp.table_01_diplom_6_310
group by shippingroute_name, rid_hash, shk_id, src_office_id, dt_start, dt_finish
having length(arr_points) > 3

--это будет инсертиться, неуверен как тут будет работать оконка, тут же вроде нет оптимизатора в клике, если она будет считаться каждый раз заново все 50 раз
-- , то это непотимально наверно тогда лучше переделать под массив заказов
insert into report.offices_over_3_points_310
select src_office_id, shippingroute_name, arr_points
    , count(rid_hash) over(partition by (src_office_id, shippingroute_name)) qty_rid
    , rid_hash, shk_id, dt_start, dt_finish
    , now() dt_load -- в питоне заменю
from tmp.table_01_diplom_main_310
limit 50 by shippingroute_name



-- 05 Запрос к витрине для Основного отчета
-- Колонки Основного отчета:
/*
 Офис оформления
 Направление, по настройкам отгрузки.
 Кол-во точек
 Кол-во товаров на данном пути
 Маршрут. Офисы, через которые проехал товар в заказе. Упорядочен по дате.
 Краснодар - Краснодар - Электросталь - Новосибирск - Иркутск
 */
 -- Фильтр: Офис оформления
 -- Фильтр: Направление
 -- Отчет показывает Топ-100 по Кол-ву точек по убыванию и с кол-вом товара более 50 штук.
select dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) src_office_name
    , shippingroute_name, length(arr_point) qty_point, qty_rid
    , replaceRegexpAll(
        toString(arrayMap(x->(dictGet('dictionary.BranchOffice','office_name', toUInt64(x))), arr_point)),
        '[\[\]\']', '') points -- если ',' заменить на '-' , то получается путаница, потому что в названии офиссов бывают '-'
from report.offices_over_3_points_310
where qty_rid > 50
order by qty_point
limit 100



-- 06 Запрос к витрине для Детализации
-- Колонки Детализация:
/*
 Офис оформления
 Направление
 Маршрут
 Дата Оформлен
 Номер заказа
 Номер товара ШК
 Дата Доставлен
 */
 -- Фильтр: Офис оформления.
 -- Фильтр: Направление.
 -- Детализация показывает не более 50 строк на одно Маршрут. src_office_id + shippingroute_name
select dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) src_office_name
    , shippingroute_name
    , replaceRegexpAll(
        toString(arrayMap(x->(dictGet('dictionary.BranchOffice','office_name', toUInt64(x))), arr_point)),
        '[\[\]\']', '') points
    , dt_start, rid_hash, shk_id, dt_finish
from report.offices_over_3_points_310
order by src_office_id, shippingroute_name
limit 50 by src_office_id, shippingroute_name