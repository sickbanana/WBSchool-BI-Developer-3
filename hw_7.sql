-- 01. Сделать витрину под Отчет выработка по смене сотрудника.
-- Партиционирование по одному дню.
-- Сортировка.
-- Выбрать движок. Учесть, что данные могут повторно заливаться в витрину.
drop table if exists report.employee_smena_310
create table report.employee_smena_310
(
    `employee_id` UInt32,
    `office_id` UInt64,
    `dt_date` Date,
    `dt_smena_start` DateTime,
    `dt_smena_end` DateTime,
    `sm_type` UInt8,
    `prodtype_id` UInt64,
    `qty_oper` UInt64,
    `amount` Decimal(38, 2)
)
engine = ReplacingMergeTree()
order by (employee_id, dt_smena_start, prodtype_id)
partition by dt_date
ttl dt_date + toIntervalDay(30)
settings index_granularity = 8192

-- 02. Заполнить витрину скриптами в редакторе. PyCharm не используем.
-- Используем таблицы: Турник, ваш агрегат по выработке.
-- При сборке используем временные таблицы. Минимум 1 шт. Через один запрос не делаем.
-- Времянка-1. Данные смен.
-- Далее можно написать селект к Времянке-1 и к таблице-агрегату.
-- Возможно в таблице Тарификатор есть смещение +3часа. Проверить на всякий случай) Если есть - то вычесть 3ч там где есть смещение.
-- Записываем только законченные смены, т.е. у которых последний выход был более 7ч назад.