-- 01
-- По каждому офису оформления src_office_id вывести кол-во qty уникальных заказов,
--   которые были в статусе Оформлен за последние 4е суток.
-- Использовать справочник имен BranchOffice для вывода имен Офисов office_name.
-- Для каждого офиса добавить колонку rid с любым номером заказа для примера. *Использовать соответсвующую агрегатную функцию.
-- Упорядочить по Кол-ву от большего к меньшему.
-- Колонки: src_office_id, office_name, qty, rid_example

select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , uniq(rid_hash) qty
    , any(rid_hash) rid_example
from history.assembly_task
where dt >= toStartOfDay(now()) - interval 4 day
group by src_office_id
order by qty desc

-- 02
-- По офису оформления src_office_id Электросталь вывести кол-во qty уникальных Сортированных заказов за каждый час dt_h,
--   за последние 4е суток.
-- Использовать справочник имен BranchOffice для вывода имен Офисов office_name.
-- В блоке фильтрации использовать идентификатор офиса Электросталь (см.справочник Офисов).
-- Использовать функцию toStartOfHour() для работы с датами для колонки dt_h.
-- Для каждого офиса добавить колонку rid с любым номером заказа для примера.
-- Добавить 2 колонки dt_min dt_max, которые показывают даты первого и последнего статуса в каждом часе.
-- Упорядочить по колонке dt_h.
-- Колонки: src_office_id, office_name, dt_h, qty, rid_example, dt_min, dt_max

select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , toStartOfHour(dt) dt_h
    , uniq(rid_hash) qty
    , any(rid_hash) rid_example
    , min(dt) dt_min
    , max(dt) dt_max
from history.sorted
where dt >= toStartOfDay(now()) - interval 4 day
    and src_office_id = 241542
group by dt_h, src_office_id
order by dt_h

-- 03
-- За 7 дней по офису Екатеринбург вывести кол-во qty уникальных заказов за каждый час.
-- Интересуют заказы отправленные на сборку. Таблица assembly_task_issued.
-- Добавить колонку hour Час заказа. Например, 14.
-- Оставить строки, в которых более 5т заказов. Также оставить строки с четными Часами в колонке hour.
-- Упорядочить по офису и dt_h.
-- Колонки: src_office_id, office_name, dt_h, qty, hour.

select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , toStartOfHour(issued_dt) dt_h
    , uniq(rid_hash) qty
    , toHour(issued_dt) hour
from history.assembly_task_issued
where create_dt >= toStartOfDay(now()) - interval 7 day
    and src_office_id = 3480
    and hour % 2 = 0
group by dt_h, src_office_id, hour
having qty > 5000
order by src_office_id, dt_h

-- 04
-- По офису Хабаровск за последние 3 дня посчитать кол-во Доставленных заказов (таблица order_completed),
--   которые были Оформлены (таблица assembly_task) в период между -7 и -3 дня.
-- Также показать 1 пример заказа в колонке rid_example.
-- Упорядочить по убыванию кол-ва.
-- Колонки: dt_date, qty, rid_example.
--  *Использовать подзапрос в блоке фильтрации для отбора заказов).

select toDate(dt) dt_date
    , uniq(rid_hash) qty
    , any(rid_hash) rid_example
from history.order_completed
where dt >= toStartOfDay(now()) - interval 3 day
    and rid_hash in
    (
        select rid_hash
        from history.assembly_task
        where dt between toStartOfDay(now()) - interval 7 day
            and toStartOfDay(now()) - interval 3 day
            and src_office_id = 2400
    )
group by dt_date
order by qty desc

