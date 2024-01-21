-- Сделать отчет Возвраты ШК.
-- Показать по каждому Офису src_office_id на каждый день Кол-во возвратов на склад оформления и Кол-во переупаковок.
-- Информацию показывать за последние 30 дней от даты статуса OCR Возврат заказа.
-- Дашборд Часть-1. График Возвраты. Колонки:
--   Офис, дата, Кол-во.
-- Дашборд Часть-2. График Переупаковка. Колонки:
--   Офис, дата, Кол-во.
-- Дашборд Часть-3. Таблица. Колонки:
--   Офис, дата, Заказ, ШК, Дата Выполнения заказа, Дата Возврат, Дата Переупаковки, Офис Переупаковки, МХ переупакрвки.
--   Вывести 5 Заказов на каждые Офис и Дату.

-- 01
-- Расписать словами как планируете реализовать данную задачу.
-- Что конкретно расписать:
-- а) что планируете хранить в витрине. в каком разрезе.
офис номер заказа, шк, дата возрата, дата возращения, дата переупаковки, офик переупаковки, MX переупаковки
-- б) сколько по времени будет храниться инфа в вашей витрине, как будет она будет удаляться.
Для отчета требуется информация за последние 30 дней,
поэтому будем хранить данные в этом промежутке, поэтому сделаем ttl toStartOfDay(dt) + toIntervalDay(30) таким же образом информация и будет удаляться
-- в) какой движок планируете использовать и почему.
replacing merge tree для того чтобы удалялись старые записи,
допустим товар был переупаковон но не доставлен, позже появилась запись с датой возращения и предыдущую запись нужно удалить
-- г) какая сортировка и почему.
Сортировка по номеру заказа и шк так как оба поля нужны в условии where
-- д) как в даге будете обновлять уже имеющуюся в витрине инфу и что конкретно обновлять.
буду проверять даты dt_return и dt_repack, если одна из них равна 0 то буду добалять новые данные по этим заказам, движок будет удалять дубликаты
-- е) как в даге будете подливать новую инфу. как сделаете инкрементальность.
создам вемянки с вдижком memory и залью инфу в витрину,
буду проверять наличие заказа в витрине(так как одна шк может быть в разных заказах) и буду брать инфу за последние сутки от макс даты в витрине.

-- 02
-- Сделать витрину для отчета.

-- 1. ReplacingMergeTree(rid_hash)
-- Для реплейсинг указываем колонку, по которой отслеживается версия. Дату или ничего не указываем.
-- В текущем случае можно ничего не указывать. Или сделать материалзованную колонку dt_load default now(), и ее указать.
-- Задачу приму, но поправь.
-- 2. Партиционирование не нужно. Потому что витрина - это небольщая таблица в 99% случаев, в данной задаче небольшая.
-- Тоже убери.
drop table if exists report.shk_return_310;
create table report.shk_return_310
(
    `src_office_id` UInt32,
    `rid_hash` UInt64,
    `shk_id` UInt64,
    `dt_ocr` DateTime,
    `dt_return` DateTime,
    `dt_repack` DateTime,
    `repack_office_id` UInt32,
    `mx_repack` UInt64
)
engine = ReplacingMergeTree()
order by (rid_hash, shk_id)
ttl toStartOfDay(dt_ocr) + toIntervalDay(30)
settings index_granularity = 8192



-- 03
-- Заполнить ее за всю историю 30 дней.
-- Квантили использовать не нужно. Достаточно выполнить весь код из 11 домашки.
-- Здесь приложить результат запроса по Кол-ву строк в вашей витрине.
insert into report.shk_return_310
select src_office_id
     , rid_hash
     , shk_id
     , dt_ocr
     , minIf(dt_mx, office_id = src_office_id) dt_return
     , minIf(dt_mx, state_id = 'WPU') dt_repack
     , argMinIf(office_id, dt_mx, state_id = 'WPU') repack_office_id
     , argMinIf(mx, dt_mx, state_id = 'WPU') mx_repack
from tmp.table11_6_310
group by rid_hash, shk_id, dt_ocr, src_office_id
having dt_repack != 0 or dt_return != 0

select count() qty
from report.shk_return_310

--999158


-- 04
-- Сделать даг.
-- github MergeRequest
-- Не забыть про пункты и Задачи-1:
-- д) как в даге будете обновлять уже имеющуюся в витрине инфу и что конкретно обновлять.
-- е) как в даге будете подливать новую инфу. как сделаете инкрементальность.
