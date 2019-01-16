# cass_to_hdfs_parquet
Scala application for loading data from Cassandra into HDFS parquet files.

build with SBT

Windows: 
 cd C:\cass_to_hdfs_parquet
 
 sbt assembly
 
 cd C:\cass_to_hdfs_parquet\target\scala-2.11
 
 it is casstohdfs_v1.jar

Copy it on cluster and run with:

spark-submit --class CassToHdfs 
--master spark://192.168.122.219:6066 
--deploy-mode cluster /root/casstohdfs_v1.jar  
--total-executor-cores 1 
--driver-memory 500M 
--executor-memory 500M 
--num-executors 1

monitoring on HDFS:

[hadoop@hdpnn ~]$ hadoop fs -rm -r /user/tickers/ticks.parquet

[hadoop@hdpnn ~]$ hadoop fs -ls /user/tickers/ticks.parquetFound 10 items</br>
drwxr-xr-x   - root root          0 2019-01-16 11:10 /user/tickers/ticks.parquet/ticker_id=1</br>
drwxr-xr-x   - root root          0 2019-01-16 11:15 /user/tickers/ticks.parquet/ticker_id=2</br>
drwxr-xr-x   - root root          0 2019-01-16 11:20 /user/tickers/ticks.parquet/ticker_id=3</br>
drwxr-xr-x   - root root          0 2019-01-16 11:23 /user/tickers/ticks.parquet/ticker_id=4</br>
...
...

Query with HIVE
drop table ticker;

create external table ticker(
 db_tsunx  BIGINT,
 ask       double,
 bid       double
) 
PARTITIONED BY (ticker_id INT,ddate DATE)
STORED AS PARQUET
LOCATION 'hdfs://hdpnn:9000/user/tickers/ticks.parquet/';

MSCK REPAIR TABLE ticker;

select * from ticker;

Article here: 
https://yakushev-bigdata.blogspot.com/2019/01/load-data-from-cassandra-to-hdfs.html
https://yakushev-bigdata.blogspot.com
