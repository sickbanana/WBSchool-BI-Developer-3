--=== Задание-2 ===--

-- Порядок колонок использовать как написано в задании. Это сокращает время проверки)
-- За последние 3е суток по офису Хабаровск id 2400 вывести 100 заказов со следующими колонками:
-- 1. rid_hash
-- 2. Дата последнего статуса Отправлен на сборку. Колонку округлить до ближайших 15 минут. См.соответсвующую функцию.
-- 3. Дата первого статуса Сортирован.
-- 4. Разница в Часах между 2мя колонками с датами.
-- 5. Посчитать кол-во уникальных ШК в заказе. Бывает ШК меняется в заказе.
-- 6. Вывести все ШК в массив. Использовать соответствующую функцию при группировке. Массив должен быть без дублей и отсортирован по возрастанию.
--    Также элементы массива должны быть не четными)
-- 7. Вывести последний элемент массива.
-- 8. Текстовая колонка: "Последний элемент массива: 4783689345"
--* join не используем)

-- Почему у некоторых заказов diff_h принимает аномально высокие или низкие значения. Написать словами почему так произошло.
-- Какое условие в запрос можно добавить, чтобы это избежать.

select rid_hash
    , maxIf(dt, src = 'assembly_task_issued') dt_issued
    , minIf(dt, src = 'sorted') dt_sorted
    , dateDiff('hour', dt_issued, dt_sorted) diff_h
    , uniq(shk_id) qty
    , arraySort(groupArrayDistinctIf(shk_id, shk_id % 2 = 1)) shk_arr
    , shk_arr[-1] item_last
from
(
    select rid_hash, shk_id, issued_dt dt, 'assembly_task_issued' src
    from history.assembly_task_issued
    where issued_dt >= toStartOfDay(now()) - interval 3 day
        and src_office_id = 2400
    union all
    select rid_hash, shk_id, dt, 'sorted' src
    from history.sorted
    where dt >= toStartOfDay(now()) - interval 3 day
        and src_office_id = 2400
)
group by rid_hash
having uniq(src) > 1
order by diff_h desc

# а having countIf зачем?  "мы проверяем, что заказ есть в обоих таблицах" - для этого? - проще надо
    
-- diff_h принимает аномально высокие или низкие значения, когда возникает ситуация, что заказ есть только в таблице sorted или assembly_task_issued.
-- Чтобы этого избежать, мы проверяем, что заказ есть в обоих таблицах.

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
order by dt, src_office_id
limit 5 by dt_date, src_office_id

# сортировки не хватает одной
# правильнее все же так 
# order by src_office_id, dt_date
# limit 5 by src_office_id, dt_date


--=== Задание-4 ===--
-- Для офисов, у которых за 3 дня процент заказов Оформлен к Сортирован в диапазоне 30-50% вывести следующую информацию.
-- За 3 дня показать 5 заказов по каждому офису за каждый день по статусу из Отмена.
-- Для вывода 5 заказов использовать оператор limit 5 by ...
-- Колонки: src_office_id, office_name, dt_date, rid_hash, nm_id, sm_id.
-- * join не используем)

select src_office_id
    , dictGet('dictionary.BranchOffice','office_name', toUInt64(src_office_id)) office_name
    , toDate(reject_dt) dt_date
    , rid_hash
    , nm_id
    , sm_id
from history.rejected
where dt_date >= toStartOfDay(now()) - interval 3 day
    and src_office_id in
        (
            select src_office_id
            from
            (
                select src_office_id, rid_hash, 'assembly_task' src
                from history.assembly_task
                where dt >= toStartOfDay(now()) - interval 3 day
                union all
                select src_office_id, rid_hash, 'sorted' src
                from history.sorted
                where dt >= toStartOfDay(now()) - interval 3 day
            )
            group by src_office_id
            having uniqIf(rid_hash, src = 'assembly_task') / uniqIf(rid_hash, src = 'sorted') between 0.3 and 0.5
        )
order by reject_dt, src_office_id
limit 5 by dt_date ,src_office_id
