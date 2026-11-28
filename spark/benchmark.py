"""Same real-trip SQL workload with explicit executor count. No fabricated speedup."""
import argparse
import time
from pyspark.sql import functions as F
from comun import sesion,HDFS,guardar_json

p=argparse.ArgumentParser()
p.add_argument('--meses',required=True)
p.add_argument('--resultados',required=True)
p.add_argument('--ejecutores',type=int,required=True)
args=p.parse_args()
spark=sesion('Measured historical benchmark '+str(args.ejecutores))
started=time.perf_counter()
paths=[HDFS+'/crudo/yellow/yellow_tripdata_'+m+'.parquet' for m in args.meses.split(',')]
data=spark.read.parquet(*paths).select('PULocationID','tpep_pickup_datetime','trip_distance','total_amount')
partition_count=data.rdd.getNumPartitions()
result=(data.groupBy('PULocationID',F.hour('tpep_pickup_datetime').alias('hour'))
        .agg(F.count('*').alias('trips'),F.sum('trip_distance').alias('miles'),F.sum('total_amount').alias('amount')).collect())
seconds=time.perf_counter()-started
fs=spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
Path=spark.sparkContext._jvm.org.apache.hadoop.fs.Path
logical=sum(fs.getContentSummary(Path(path)).getLength() for path in paths)
n=sum(r['trips'] for r in result)
guardar_json(args.resultados,'benchmark_'+str(args.ejecutores)+'.json',{
    'executor_instances':spark.sparkContext.getConf().get('spark.executor.instances'),
    'months':args.meses.split(','),'records':n,'input_bytes':logical,'partitions':partition_count,
    'seconds':seconds,'rows_per_second':n/seconds,'aggregate_cells':len(result),
    'workload':'Identical zone/hour SQL count and sums of distance and total fare over real Parquet',
    'limitations':'One pass per executor count; first run can warm OS/HDFS caches. Not a proof of absolute necessity or a controlled repeated scaling study.'})
spark.stop()
