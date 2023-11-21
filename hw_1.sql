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
where date_diff('day', dt, now()) < 4
    # нет, так нельзя. код рабочий, но так не делаем 
group by src_office_id
order by qty desc;

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
where src_office_id = 241542 and date_diff('day', create_dt, now()) < 4
group by dt_h, src_office_id
order by dt_h
