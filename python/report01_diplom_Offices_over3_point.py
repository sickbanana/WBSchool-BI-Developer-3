import datetime
import json
from clickhouse_driver import Client

dbname = 'agg'
dst_table = 'report.offices_over_3_points_310'


with open('secrets/ch.json') as json_file:
    data = json.load(json_file)

client = Client(data['server'][0]['host'],
                user=data['server'][0]['user'],
                password=data['server'][0]['password'],
                port=data['server'][0]['port'],
                verify=False,
                database=dbname,
                settings={"numpy_columns": False, 'use_numpy': False},
                compression=True)

dt_load = datetime.datetime.now()

quantiles_query = f"""
    select qvan4
    from
    (
        select arrayPushFront(qvan3, min(rid_hash)) qvan4
             , length(qvan4) qty
             , quantiles(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)(rid_hash) qvan
             , arrayMap(x -> toUInt64(x), qvan) qvan2
             , arrayPushBack(qvan2, max(rid_hash)) qvan3
        from history.order_completed
    )
    """

quantile_result = client.execute(quantiles_query)[0][0]

for i in range(0, len(quantile_result) - 1):
    rid_start = int(quantile_result[i])
    rid_end = int(quantile_result[i + 1])

    print(f"Итерация: {i + 1}. Обрабатываются заказы: {rid_start} - {rid_end}.")

    client.execute(f"""drop table if exists tmp.table_01_diplom_1_310""")

    table1_query = f"""                    
            create table tmp.table_01_diplom_1_310 engine Memory() as
            select rid_hash, max(dt) dt_finish
            from history.order_completed
            where dt >= now() - interval 5 day
                and rid_hash between {rid_start} and {rid_end}
            group by rid_hash
        """

    client.execute(table1_query)

    client.execute(f"""drop table if exists tmp.table_01_diplom_2_310""")

    table2_query = f"""                    
            create table tmp.table_01_diplom_2_310 engine Memory() as
            select rid_hash, shk_id, max(dt) dt_start
            from history.assembled
            where dt >= now() - interval 30 day
                and rid_hash in (select rid_hash from tmp.table_01_diplom_1_310)
            group by rid_hash, shk_id
        """

    client.execute(table2_query)

    client.execute(f"""drop table if exists tmp.table_01_diplom_3_310""")

    table3_query = f"""              
            create table tmp.table_01_diplom_3_310 engine Memory() as      
            select rid_hash, src_office_id, dst_office_id
                , dictGet('dictionary.OutfitAssemblySettings','shippingroute_id', (toUInt64(src_office_id), toUInt64(dst_office_id))) shippingroute_id
            from history.assembly_task_issued
            where issued_dt >= now() - interval 30 day
                and rid_hash in (select rid_hash from tmp.table_01_diplom_2_310)
        """

    client.execute(table3_query)

    client.execute(f"""drop table if exists tmp.table_01_diplom_4_310""")

    table4_query = f"""  
            create table tmp.table_01_diplom_4_310 engine Memory() as                  
            select t1.rid_hash rid_hash, src_office_id, dst_office_id, shk_id, dt_start, dt_finish
                , dictGet('dictionary.ShippingRoute','shippingroute_name', shippingroute_id) shippingroute_name
            from tmp.table_01_diplom_1_310 t1
            semi join tmp.table_01_diplom_2_310 t2
            on t1.rid_hash = t2.rid_hash
            semi join tmp.table_01_diplom_3_310 t3
            on t1.rid_hash = t3.rid_hash
        """

    client.execute(table4_query)

    client.execute(f"""drop table if exists tmp.table_01_diplom_5_310""")

    table5_query = f"""       
            create table tmp.table_01_diplom_5_310 engine Memory() as             
            select item_id shk_id, dt, mx
                , dictGet('dictionary.StoragePlace','office_id', toUInt64(mx)) office_id
            from history.ShkOnPlace
            where shk_id in (select shk_id from tmp.table_01_diplom_4_310)
        """

    client.execute(table5_query)

    client.execute(f"""drop table if exists tmp.table_01_diplom_6_310""")

    table6_query = f"""       
            create table tmp.table_01_diplom_6_310 engine Memory() as             
            select shippingroute_name, rid_hash, t5.shk_id shk_id, src_office_id, dst_office_id, dt_start, dt_finish, office_id, dt
            from tmp.table_01_diplom_5_310 t5
            asof join tmp.table_01_diplom_4_310 t4
            on t5.shk_id = t4.shk_id and dt > dt_start
            where dt < dt_finish
                and office_id != src_office_id
                and office_id != dst_office_id
        """

    client.execute(table6_query)

    client.execute(f"""drop table if exists tmp.table_01_diplom_main_310""")

    table_main_query = f"""       
            create table tmp.table_01_diplom_main_310 engine Memory() as             
            select shippingroute_name, arr4 arr_points, rid_hash, shk_id, src_office_id, dt_start, dt_finish
                , arraySort(groupArray((dt, office_id))) arr1
                , arrayFilter(x -> x.2 != 0, arr1) arr2 -- из-за нулевых офиссов после их удаления могут быть повторения, поэтому удаляю предварительно тоже, есть вариант присваивать не 0 а -1 тогда в функции ниже
                , arrayMap(x -> (if(arr2[x].2 != arr2[x+1].2, arr2[x].2, 0)), arrayEnumerate(arr1)) arr3
                , arrayFilter(x -> x != 0, arr3) arr4
            from tmp.table_01_diplom_6_310
            group by shippingroute_name, rid_hash, shk_id, src_office_id, dt_start, dt_finish
            having length(arr_points) > 3
        """

    client.execute(table_main_query)


insert_query = f"""       
insert into report.offices_over_3_points_310
select src_office_id, shippingroute_name, arr_points , qty_rid, rid_hash, shk_id, dt_start, dt_finish, now() dt_load -- в питоне заменю
from
(
    select src_office_id
      , shippingroute_name
      , arr_points
      , count(rid_hash) over (partition by (src_office_id, shippingroute_name)) qty_rid
      , rid_hash
      , shk_id
      , dt_start
      , dt_finish
    from tmp.table_01_diplom_main_310
)
where qty_rid > 50
limit 50 by shippingroute_name
    """

client.execute(insert_query)

print(f"Витрина заполнена.")

