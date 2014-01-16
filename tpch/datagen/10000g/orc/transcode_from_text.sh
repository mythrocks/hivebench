#!/bin/bash

echo "Transcoding 10000GB data from Text to ORC format..."
hive -f transcode_6_smaller_tables.hive


echo "Transcoding 10000GB lineitem table from Text to ORC format..."
hive -e "select distinct(l_shipdate) from mythdb_10000g_text.lineitem order by l_shipdate" > lineitem.shipdates.txt
../../chunk.py lineitem.shipdates.txt 200 "set hive.merge.mapredfiles=true; set hive.merge.mapfiles=true;  from mythdb_10000g_text.lineitem insert overwrite table mythdb_10000g_orc.lineitem_partitioned_shipdate partition( L_SHIPDATE ) select L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT, L_SHIPDATE WHERE L_SHIPDATE >= '\$replaceme1' AND L_SHIPDATE <= '\$replaceme2';"


echo "Transcoding 10000GB orders table from Text to ORC format..."
hive -e "select distinct(o_orderdate) from mythdb_10000g_text.orders order by o_orderdate" > orders.orderdates.txt
../../chunk.py orders.orderdates.txt 200 "set hive.merge.mapredfiles=true; set hive.merge.mapfiles=true; from mythdb_10000g_text.orders insert overwrite table mythdb_10000g_orc.orders_partitioned_orderdate partition (O_ORDERDATE) select  O_ORDERKEY, O_CUSTKEY , O_ORDERSTATUS , O_TOTALPRICE , O_ORDERPRIORITY , O_CLERK , O_SHIPPRIORITY , O_COMMENT , O_ORDERDATE where O_ORDERDATE >= '\$replaceme1' AND O_ORDERDATE <= '\$replaceme2'; "
