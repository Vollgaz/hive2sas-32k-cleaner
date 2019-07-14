# **parquet2hive**
Correct the '32K Bug' of SAS. In the case of string column SAS will try to build each cell with 32k characters.
It increases the storage used by the dataset and the execution time.

# Standalone
## **Execution parameters**
Parameters are passe  --key=value  \
--type : 'parquet' or 'csv'\
--hiveDb : Hive database to write\
--file : Global path on HDFS to the file
--sep : (Required) column separator in case you try to analyse a CSV file.

## **Examples in standalone**
Case of parquet file in standalone mode.
>spark-submit ..... parquet2hive.jar  --hiveDb=infocentre_mysql --type=parquet --file=/global/Path/To/File

Case of CSV (semi-colon separated) in standalone mode.
>spark-submit ..... parquet2hive.jar --hiveDb=infocentre_mysql --type=csv --sep=; --file=/global/Path/To/File

# **Project dependence.**
First, it is required to write down the dataframe in a parquet file.

- Spark-submit in cluster mode. In case the workers are not configured as hive-client, you need to distribute the hive-site.xml when you execute spark-submit
> ...--deploy-mode cluster --files /usr/hdp/current/spark2-client/conf/hive-site.xml --conf ...

- sparksession creation
> SparkSession.builder()**.enableHiveSupport()**.getOrCreate

- Parquet analysis : 
> import org.vollgaz.spark.analysis._ \
> val filepath = _**s"global/path/to/parquet"**_\
> val df = new SourceParquet( _**filepath**_ ).process()\
> val hiveCaller = new HiveQuery( _**hivedb**_, _**filepath**_ )\
> spark.sql( hiveCaller.queryDropTable() )\
> spark.sql( hiveCaller.queryCreateTable( _**df.collect()**_ ) )
