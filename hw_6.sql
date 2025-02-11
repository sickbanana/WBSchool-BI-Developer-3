-- 02. Исследование таблицы тарификатор.
-- а) Придумать 5 запросов.

--кол-во записей в таблице
select count() qty
from history.calc

--кол-во сотрудников в каждом оффисе
    -- не стоит работать со всей таблицей, выбери промежуток
    -- now() тоже не стоит использовать, у тебя получается - 24 часа, а не вес предыдущий день
select office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(office_id)) office_name
    , uniq(employee_id) qty
from history.calc
where dt >= toStartOfDay(now()) - interval 10 day
group by office_id

select ProdTypePart_name
from dict_prodType
group by ProdTypePart_name

--10 самых оплачиваемых операций по сборке
    -- в таблице помимо выплат присутствуют штрафы
    -- штраф как-то не особо может быть оплачиваемым, скорее взымаемым
select prodtype_id
    , dictGet('dictionary.ProdType','prodtype_name', prodtype_id) prodtype_name
    , round(avg(amount)) avg_amount
from history.calc
where dictGet('dictionary.ProdType','ProdTypePart_id', prodtype_id) = 1
    and prodtype_id between 1001 and 1204
    or prodtype_id = 40006
    -- не обязательно, можешь посмотреть статусы 4001-4071
    -- лучше выбери определеные статусы сборки и по ним смотри
group by prodtype_id
order by avg_amount desc

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

    -- по каким именно дням недели ещё дай ответ
--Движок - mergeTree, сортировка по id операции и локальному времени,
--Партиционирование по неделям, записи входит в одну партицию если у них dt лежит в одной неделе, неделя считается с понедельника по воскресенье так как mode = 1,
--TTL - 3 недели, если dt будет отставать больше чем на 3 недели запись будет удалена.



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

select toStartOfDay(max(dt)) + interval 1 day
    , date_diff('day', min(dt), max(dt))
from history.calc;


with
(select toStartOfDay(max(dt)) + interval 1 day
from history.calc
)
as max_d
select office_id
     , employee_id
     , toStartOfHour(dt) dt_h
     , toStartOfHour(msk_dt) dt_h_msk
     , prodtype_id
     , count(dt) qty_oper
     , sum(amount) amount
     , calc_date
from history.calc
where dt >= max_d - interval 2 day and dt < max_d - interval 1 day
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

