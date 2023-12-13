-- 01. Сделать витрину под Отчет выработка по смене сотрудника.
-- Партиционирование по одному дню.
-- Сортировка.
-- Выбрать движок. Учесть, что данные могут повторно заливаться в витрину.
drop table if exists report.employee_smena_310
create table report.employee_smena_310
(
    `employee_id` UInt32,
    `office_id` UInt64,
    `dt_date` Date,
    `dt_smena_start` DateTime,
    `dt_smena_end` DateTime,
    `sm_type` UInt8,
    `prodtype_id` UInt64,
    `qty_oper` UInt64,
    `amount` Decimal(38, 2)
)
engine = ReplacingMergeTree()
order by (employee_id, dt_smena_start, prodtype_id)
partition by dt_date
ttl dt_date + toIntervalDay(30)
settings index_granularity = 8192

-- 02. Заполнить витрину скриптами в редакторе. PyCharm не используем.
-- Используем таблицы: Турник, ваш агрегат по выработке.
-- При сборке используем временные таблицы. Минимум 1 шт. Через один запрос не делаем.
-- Времянка-1. Данные смен.
-- Далее можно написать селект к Времянке-1 и к таблице-агрегату.
-- Возможно в таблице Тарификатор есть смещение +3часа. Проверить на всякий случай) Если есть - то вычесть 3ч там где есть смещение.
-- Записываем только законченные смены, т.е. у которых последний выход был более 7ч назад.

-- высчитываю смены по анологии как в 4 дз, также расчитываю тип смены
create temporary table temp_smena as
select employee_id
    , dt dt_smena_start
    , if(any(dt_prev) over (rows between 1 following and 1 following) as dt_next = '1970-01-01 00:00:00'
            or any(is_in_prev) over (rows between 1 following and 1 following) = 1
            or any(employee_id) over (rows between 1 following and 1 following) != employee_id
        , dt_smena_start + interval 12 hour, dt_next) dt_smena_end
    , if(dt_smena_start + interval round(date_diff('hour', dt_smena_start, dt_smena_end) / 2) hour between toStartOfDay(dt_smena_start) + interval 8 hour
            and toStartOfDay(dt_smena_start) + interval 20 hour
        , 1, 0) sm_type
from
(
    select employee_id, dt, is_in
    , any(dt) over (partition by employee_id order by dt rows between 1 preceding and 1 preceding) dt_prev
    , any(is_in) over (partition by employee_id order by dt rows between 1 preceding and 1 preceding) is_in_prev
    from history.turniket
    -- типо такого?
    where dt >= now() - interval 30 day
)
# вообще такое НЕЛЬЗЯ делать - по всей таблице считать оконку!
# обязательно нужен фильтр какой то
where date_diff('hour', dt_prev, dt) > 7
    and is_in = 1

insert into report.employee_smena_310
select employee_id, office_id
    , toDate(dt_smena_start) dt_date
    , dt_smena_start, dt_smena_end, sm_type, prodtype_id
    , sum(qty_oper) qty_oper
    , sum(amount) amount
from agg.calc_by_dth_emp_310 a
asof join temp_smena t
on a.employee_id = t.employee_id and dt_h - interval 3 hour >= dt_smena_start
group by employee_id, office_id, dt_smena_start, dt_smena_end, sm_type, prodtype_id


drop table if exists temp_smena
