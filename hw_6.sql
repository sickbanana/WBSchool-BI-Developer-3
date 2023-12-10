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

