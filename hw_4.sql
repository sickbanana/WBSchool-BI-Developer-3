-- ДЗ.
-- Задание 1. Исследовать таблицу турникет history.turniket.

-- Сколько записей в таблице.
select count() qty_all
from history.turniket

-- Сколько записей на каждый день.
select toDate(dt) dt_date
    , count() qty_day
from history.turniket
group by dt_date

-- Сколько записей на каждый день по каждому офису.
select toDate(dt) dt_date, office_id
    , dictGet('dictionary.BranchOffice','office_name', office_id) office_name
    , count() qty_day
from history.turniket
group by  dt_date, office_id
order by dt_date, office_id

-- Сколько уникальных сотрудников в таблице.
select uniq(employee_id) qty_employee
from history.turniket

-- Сколько уникальных сотрудников на каждый день.
select toDate(dt) dt_date
    , uniq(employee_id) qty_employee_day
from history.turniket
group by dt_date

-- Сколько уникальных сотрудников на каждый день по каждому офису.
select toDate(dt) dt_date, office_id
    , dictGet('dictionary.BranchOffice','office_name', office_id) office_name
    , uniq(employee_id) qty_employee_day
from history.turniket
group by  dt_date, office_id
order by dt_date, office_id

-- Кол-во входов.
-- Кол-во выходов.
-- Среднее кол-во входов на каждого сотрудника.
-- Среднее кол-во выходов на каждого сотрудника.
select countIf(is_in, is_in = 1) qty_in
    , countIf(is_in, is_in = 0) qty_out
    , qty_in / uniq(employee_id) avg_in
    , qty_out / uniq(employee_id) avg_out
from history.turniket
# это все одним запросом надо, а не 4
