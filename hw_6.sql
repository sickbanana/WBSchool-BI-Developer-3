-- 02. Исследование таблицы тарификатор.
-- а) Придумать 5 запросов.

--кол-во записей в таблице
select count() qty
from history.calc

--кол-во сотрудников в каждом оффисе
select office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(office_id)) office_name
    , uniq(employee_id) qty
from history.calc
group by office_id

--10 самых оплачиваемых операций
select prodtype_id
    , dictGet('dictionary.ProdType','prodtype_name', prodtype_id) prodtype_name
    , round(avg(amount)) avg_amount
from history.calc
group by prodtype_id
order by avg_amount desc
limit 10

--10 самых частых операций
select prodtype_id
    , dictGet('dictionary.ProdType','prodtype_name', prodtype_id) prodtype_name
    , count() qty
from history.calc
group by prodtype_id
order by qty desc
limit 10

--сколько операцй в среднем в час делают сотрудники
select office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(office_id)) office_name
    , round(avg(qty)) avg_h
from
(
    select office_id
        , count() qty
    from history.calc
    group by office_id, employee_id, toStartOfHour(dt)
)
group by office_id

-- б) Описать словами структуру таблицы: движок, сортировку, партиционирование, TTL.

--Движок - mergeTree, сортировка по id операции и локальному времени, партиционирование по неделям, TTL - 3 недели, если dt будет отставать больше чем на 3 недели запись будет удалена.



-- 03. Сделать таблицу со своим номером в схеме agg. Пример: agg.calc_by_dth_emp_3__
-- Добавить колонку dt_date от dt_h.
-- Подобрать сортировку.
-- Сделать партиционирование по одному дню от dt_h.
-- Сделать TTL 30 дней.

drop table if exists agg.calc_by_dth_emp_310
create table agg.calc_by_dth_emp_310
(
    `office_id` UInt64,
    `employee_id` UInt32,
    `dt_h` DateTime,
    `dt_h_msk` DateTime,
    `prodtype_id` UInt64,
    `qty_oper` UInt64,
    `amount` Decimal(38, 2),
    `calc_date` Date,
    `dt_date` Date materialized toDate(dt_h)
)
engine = ReplacingMergeTree()
order by (employee_id, prodtype_id, dt_h)
partition by toStartOfDay(dt_h)
ttl toStartOfDay(dt_h) + interval 30 day
settings index_granularity = 8192

-- 04. Заполнить витрину через Python за все дни.
-- Заполнять итеративно по суткам.
-- Сколько строк получилось.
-- Скрипт выложить в гит.

select office_id
     , employee_id
     , toStartOfHour(dt) dt_h
     , toStartOfHour(msk_dt) dt_h_msk
     , prodtype_id
     , count(dt) qty_oper
     , sum(amount) amount
     , calc_date
from history.calc
where dt between now() - interval 2 day and now() - interval 1 day
group by prodtype_id, dt_h, dt_h_msk, employee_id, office_id, calc_date

-- 05. Сделать даг на инкрементальное пополнение витрины.
-- Заполнять начиная с последнего часа dt_h_msk, который есть в витрине.
-- Расписание: каждые 20 минут. Разрешенный диапазон расписания 5-55 минут.
-- Почему не используем выполнение дагов во время, которое кратно 5 минутам.
-- Почему исключаем диапазон рядом с началом каждого часа.

select office_id
     , employee_id
     , toStartOfHour(dt) dt_h
     , toStartOfHour(msk_dt) dt_h_msk
     , prodtype_id
     , count(dt) qty_oper
     , sum(amount) amount
     , calc_date
from history.calc
where dt_h_msk >= (select max(dt_h_msk) from agg.calc_by_dth_emp_310)
group by prodtype_id, dt_h, dt_h_msk, employee_id, office_id, calc_date

