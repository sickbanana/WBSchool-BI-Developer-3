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
        , is_in
        , any(row_number) over (rows between 1 following and 1 following) - row_number qty
    from
    (
        select employee_id, is_in
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

# например 454407
# я проверяю, что началась последовательность входов или выходов подряд, хотя она может состоять из 1 элемента

#Антон предложил написать альтернотивное решение, могу переделать на массвы, я так понимаю это более предпочтительное решение

drop table if exists seq

-- 03
-- Сделать запрос для расчета смен. Применить asof join.
-- Показать запрос для какого-нибудь 1го сотрудника, у которого много выходов-выходов
--   и который часто выходит в течении смены "Подышать воздухом".
-- Критерии смены:
-- Выход(событие-1) и Вход(событие-2), между которыми более 7 часов. Вход(событие-2) является началом смены.
-- Вход(событие-1) и Вход(событие-2), между которыми более 12+7 часов. Вход(событие-2) является началом смены.
-- а)  При расчете смен в строке получаем выход предыдущей смены и вход в новой смене. Но это не окончательный вид, который нужен.
--     Нам в результате нужно, чтобы начало и конец смены относились к одной смене и были в одной строке.
--     Правильный вид строки со сменой: employee_id ______, dt_smena_start '2023-11-25 11:15:23', dt_smena_end '2023-11-25 20:25:47'.
-- б)* Учесть нюанс. Человек мог выйти на работу в первый рабочий день, т.е. в выборке нет предыдущего событие-1.
--     Но вход все равно есть, и это начало смены. Нужно не потерять эту смену.
-- в)* Учесть нюанс. Человек мог прийти на работу и до сих пор работать, т.е. текущая смена еще не закончена.
--     Нужно не потерять эту смену.


with shift as
(
select employee_id
    , dt_action
    , r.is_in
    , dt_in
    , l.is_in
from
(
    select employee_id, dt dt_in, is_in
    from history.turniket
    where dt >= now() - interval 30 day
        and employee_id = 4629
        and is_in = 1
    ) l
left asof join
(
    select employee_id, dt dt_action, is_in
    from history.turniket
    where dt >= now() - interval 30 day
        and employee_id = 4629
    limit 100
) r
on r.employee_id = l.employee_id and r.dt_action < l.dt_in
where date_diff('hour', dt_action, dt_in) > 7
)
select argMin(employee_id, l.dt_in) employee_id
    , min(l.dt_in) dt_smena_start
    , if(dt_action = '1970-01-01 00:00:00'
        or argMin(r.is_in, l.dt_in) = 1, dt_smena_start + interval 12 hour
        , argMin(r.dt_action, l.dt_in) as dt_action) dt_smena_end
from
(
    select employee_id, dt_in, is_in
    from shift
) l
left asof join
(
    select employee_id, dt_action, is_in
    from shift
) r
on r.employee_id = l.employee_id and r.dt_action > l.dt_in
group by toDate(l.dt_in)
order by dt_smena_start

#employee_id = 4629 - что то с этим плохо работает,черезчур много 'смен' у него

-- 04
-- Сделать запрос для расчета смен. Применить оконную функцию.


-- 05
-- Сделать запрос для расчета смен. Решить через массивы и лямбда-выражение.

-- 06*
-- Посчитать среднее время длительности смены по каждому офису и за каждый день.

-- 07*
-- В какие часы происходит наибольшее начало смен по каждому офису. Вывести топ-3 часов по каждому офису.
-- В какие часы происходит наибольшее окончание смен по каждому офису. Вывести топ-3 часов по каждому офису.
