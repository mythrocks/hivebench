#!/bin/bash

DATA_DIR=/hivebench/1000g/text

echo "Generating text data in $DATA_DIR"
hadoop jar tpch-gen/target/tpch-gen-1.0-SNAPSHOT.jar   -d $DATA_DIR -p 1000 -s 1000

echo "Removing superfluous files from 'nation'"
hdfs dfs -mv $DATA_DIR/nation/data-m-00000 $DATA_DIR/nation/one_copy_data-m-00000
hdfs dfs -rm -skipTrash $DATA_DIR/nation/data-m-0*

echo "Removing superfluous files from 'region'"
hdfs dfs -mv $DATA_DIR/region/data-m-00000 $DATA_DIR/region/one_copy_data-m-00000
hdfs dfs -rm -skipTrash $DATA_DIR/region/data-m-0*
