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
# это отдельным запросом
-- Кол-во входов.
-- Кол-во выходов.
-- Среднее кол-во входов на каждого сотрудника.
-- Среднее кол-во выходов на каждого сотрудника.
select toDate(dt) dt_date, office_id
    , dictGet('dictionary.BranchOffice','office_name', office_id) office_name
    , uniq(employee_id) qty_emp_day
    , countIf(is_in, is_in = 1) qty_in
    , countIf(is_in, is_in = 0) qty_out
    , round(qty_in / qty_emp_day) avg_in
    , round(qty_out / qty_emp_day) avg_out
from history.turniket
group by  dt_date, office_id
order by dt_date, office_id
# это все одним запросом надо, а не 4

-- 02
-- Посчитать кол-во входов или выходов, которые были подряд у одного сотрудника.
-- Если у сотрудника есть 5 входов подряд без единого выхода, тогда выводим 5.
-- Колонки: employee_id 9847593487, qty_in 5. (Это пример результата)
-- Выбрать одного сотрудника, у которого есть несколько входов или выходов подряд. -- В запросе его использовать.

--Решил предложить альтернотивное решение через оконные функции
create temporary table seq as
(
    select employee_id
        , dt
        , is_in
        , row_number
        , is_in_prev
        , any(row_number) over (rows between 1 following and 1 following) - row_number qty
    from
    (
        select employee_id, dt, is_in
            , row_number() over (order by dt) row_number
            , any(is_in) over (rows between 1 preceding and 1 preceding) is_in_prev
        from history.turniket
        where employee_id = 25317
    )
    where (is_in = 0 and is_in_prev = 1)
        or (is_in = 1 and is_in_prev = 0)
)
--входов подряд
select employee_id, qty qty_in
from seq
where qty_in > 1
    and is_in = 1
--выходов подряд
select employee_id, qty qty_out
from seq
where qty_out > 1
    and is_in = 0

# есть другие примеры employee_id?
# where (is_in = 0 and is_in_prev = 1) or (is_in = 1 and is_in_prev = 0) - это условие на корректность вход\выход.
# в seq у тебя избыточность данных - это усложняет.

drop table if exists seq

