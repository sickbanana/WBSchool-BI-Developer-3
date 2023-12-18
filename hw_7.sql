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

# важный момент - где данных больше в agg.calc_by_dth_emp_310 или в temp_smena?

select count() from agg.calc_by_dth_emp_310
--14715463

select count() from temp_smena
--8138481

-- по идеи тогда у меня правильно стоит? я об этом не подумал сразу, в следующий раз учту

drop table if exists temp_smena

-- 03.
-- Написать даг на инкрементальное пополенение витрины.
insert into report.employee_smena_310
select employee_id, office_id
    , toDate(dt_smena_start) dt_date
    , dt_smena_start, dt_smena_end, sm_type, prodtype_id
    , sum(qty_oper) qty_oper
    , sum(amount) amount
from (
    select office_id, employee_id, dt_h, prodtype_id, qty_oper, amount
    from agg.calc_by_dth_emp_310
    where dt_h >= (select max(dt_smena_start) from {dst_table} final) - - interval 3 day + interval 3 hour
) a
asof join {tmp_table} t
on a.employee_id = t.employee_id and dt_h - interval 3 hour >= dt_smena_start
group by employee_id, office_id, dt_smena_start, dt_smena_end, sm_type, prodtype_id


-- 04.
-- Написать запрос для будущего дашборда.
-- Выработка в контексте каждой даты и офиса за последние 10 дней.
-- Используем таблицу агрегат.
select office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(office_id)) office_name
    , dt_date
    , sum(qty_oper) production
from report.employee_smena_310 final
where dt_date >= today() - interval 10 day
group by dt_date, office_id
order by dt_date, office_id

-- 05.
-- Написать запрос для будущего дашборда.
-- Выработка в контексте каждой даты, офиса и участка работ за последние 10 дней.
-- Используем таблицу агрегат.
select dt_date, office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(office_id)) office_name
    , dictGet('dictionary.ProdType','ProdTypePart_name', prodtype_id) ProdTypePart_name
    , sum(qty_oper) production
from report.employee_smena_310 final
where dt_date >= today() - interval 10 day
group by dt_date, office_id, ProdTypePart_name
order by dt_date, office_id, ProdTypePart_name

-- 06.
-- Написать запрос для будущего дашборда.
-- За последние 15 дней Вывести данные Смена начало, Смена конец, Участок работ, Сотрудник, Сумма выработки,
--   для 100 сотрудников, у которых самая большая выработка(за последние 10 дней), и у которых было более 2х участков работ(за последние 10 дней).
-- Используем свою витрину.
select dt_smena_start, dt_smena_end
    , dictGet('dictionary.ProdType','ProdTypePart_name', prodtype_id) ProdTypePart_name
    , employee_id
    , sum(qty_oper) qty_oper
from report.employee_smena_310 final
where dt_date >= today() - interval 15 day and employee_id in
(
    select employee_id
    from report.employee_smena_310 final
    where dt_date >= today() - interval 10 day
    group by employee_id
    having uniq(dictGet('dictionary.ProdType','ProdTypePart_name', prodtype_id)) >= 2
    order by sum(qty_oper) desc
    limit 100
)
group by dt_smena_start, dt_smena_end, ProdTypePart_name, employee_id
order by dt_smena_start, dt_smena_end, ProdTypePart_name, employee_id



-- 07. Перенести отчет Заказы 8 часов в Superset.
-- Слой 1. График Кол-во по дням и по офисам.
-- Слой 2. Таблица Кол-во по дням и по офисам.
-- Слой 3. Топ-1000 самых отстающих заказов.
-- Дать нормальные имена колонкам. Колонки настраиваются в разделе Dataset.
-- Добавить фильтр по офисам.

alter table report.orders_not_in_assembly_310 delete
where dt_last >= (select max(dt_last)
    from report.orders_not_in_assembly_310 final
    where is_deleted = 0) - interval 24 hour

-- Слой 1. График Кол-во по дням и по офисам.
select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) src_office_name
    , uniq(rid_hash) qty_rids
    , toDate(dt_last) dt_date
from report.orders_not_in_assembly_310 final
where is_deleted = 0
group by src_office_id, dt_date
order by src_office_id, dt_date

-- Слой 3. Топ-1000 самых отстающих заказов.
select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) src_office_name
    , toUInt32(rid_hash) rid_hash
    , dst_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(dst_office_id)) dst_office_name
from report.orders_not_in_assembly final
where is_deleted = 0
order by dateDiff('hour', dt_last, now()) desc
limit 1000
