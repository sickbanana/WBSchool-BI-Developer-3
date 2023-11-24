--=== Задание-3 ===--
-- Для офисов, у которых за 3 дня было между 10т и 50т заказов в статусе Оформлен, вывести следующую информацию.
-- За 3 дня показать 5 заказов по этим офисам за каждый день, которые были в статусе Сортирован.
-- Для вывода 5 заказов использовать оператор limit 5 by ...
-- Колонки: src_office_id, office_name, dt_date, rid_hash, shk_id.
-- * join не используем)

select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , toDate(dt) dt_date
    , rid_hash
    , shk_id
from history.sorted
where dt >= toStartOfDay(now()) - interval 3 day
    and src_office_id in
    (
        select src_office_id
        from history.assembly_task
        where dt >= toStartOfDay(now()) - interval 3 day
        group by src_office_id
        having uniq(rid_hash) between 10000 and 50000
    )
limit 5 by src_office_id, dt_date

# limit чаще всего используется с еще одним операндом, у тебя его не вижу
