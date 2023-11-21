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
where date_diff('day', create_dt, now()) <= 4
group by src_office_id
order by qty desc;