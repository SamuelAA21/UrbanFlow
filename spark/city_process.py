"""Immutable microbatch snapshot, trip lifecycle aggregates and scoped ML models."""
import argparse
import datetime
import os
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType,IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from comun import sesion,HDFS,guardar_json,guardar_csv,Cronometro

p=argparse.ArgumentParser()
p.add_argument('--resultados',required=True)
args=p.parse_args()
spark=sesion('City events and reported demand')
spark.conf.set('spark.sql.session.timeZone','America/New_York')
job=int(os.environ['URBANFLOW_CITY_JOB'])
high=int(os.environ['URBANFLOW_CITY_HIGH'])
low=int(os.environ.get('URBANFLOW_CITY_LOW','0'))
previous=os.environ.get('URBANFLOW_CITY_PREVIOUS')
base=HDFS+f'/city/runs/{job}'
schema=StructType([StructField(k,t) for k,t in [
    ('row_id',LongType()),('event_id',StringType()),('trip_id',StringType()),('kind',StringType()),
    ('source',StringType()),('event_time',StringType()),('pickup_time',StringType()),
    ('dropoff_time',StringType()),('origin',IntegerType()),('destination',IntegerType()),
    ('distance_mi',DoubleType()),('fare',DoubleType())]])
input_paths=json.loads(os.environ['URBANFLOW_CITY_PATHS'])
incoming=(spark.read.schema(schema).json(input_paths)
          .filter((F.col('row_id')>low)&(F.col('row_id')<=high)))
new_rows=incoming.count()
events=incoming.unionByName(spark.read.parquet(previous+'/events')) if previous else incoming
events=events.dropDuplicates(['event_id']).cache()
events.write.mode('overwrite').parquet(base+'/events')
counts={r['source']+':'+r['kind']:r['count'] for r in events.groupBy('source','kind').count().collect()}
completed=(events.filter(F.col('kind')=='completed')
           .withColumn('fecha',F.to_date(F.to_timestamp('pickup_time'))).withColumn('hora',F.hour(F.to_timestamp('pickup_time')))
           .withColumnRenamed('origin','zona_origen'))
hourly=(completed.groupBy('source','fecha','hora','zona_origen').agg(F.count('*').alias('viajes')))
hourly.write.mode('overwrite').partitionBy('source').parquet(base+'/reported_hourly')
historical=spark.read.parquet(HDFS+'/agregado/zona_hora').select('fecha','hora','zona_origen','viajes')
variables=['zona_origen','hora','weekday','day_index']
first=historical.agg(F.min('fecha')).first()[0]
forecasts=[]
training={}
with Cronometro() as timer:
    for source in ('simulated','driver_report'):
        reported=hourly.filter(F.col('source')==source).drop('source')
        n=reported.count()
        if n<24:
            continue
        # Only completed reports are labels. Requests/cancellations never count as trips.
        data=historical.unionByName(reported).groupBy('fecha','hora','zona_origen').agg(F.sum('viajes').alias('viajes'))
        data=data.withColumn('weekday',F.dayofweek('fecha')).withColumn('day_index',F.datediff('fecha',F.lit(first)))
        model=Pipeline(stages=[VectorAssembler(inputCols=variables,outputCol='features'),
                              RandomForestRegressor(labelCol='viajes',numTrees=20,maxDepth=8,maxBins=256,seed=42)]).fit(data)
        model.write().overwrite().save(base+'/models/'+source)
        last=reported.agg(F.max('fecha')).first()[0]
        target=last+datetime.timedelta(days=1)
        zones=data.select('zona_origen').distinct()
        hours=spark.range(24).select(F.col('id').cast('int').alias('hora'))
        future=(zones.crossJoin(hours).withColumn('fecha',F.lit(target)).withColumn('weekday',F.dayofweek('fecha'))
                .withColumn('day_index',F.datediff('fecha',F.lit(first))))
        predictions=(model.transform(future).select(F.col('fecha').alias('day'),F.col('hora').alias('hour'),
                     F.col('zona_origen').alias('zone'),F.greatest(F.lit(0.),F.col('prediction')).alias('estimate')))
        for r in predictions.collect():
            forecasts.append((r['day'],r['hour'],r['zone'],r['estimate'],source))
        training[source]={'historical_and_reported_cells':data.count(),'new_reported_cells':n,
                          'model_path':base+'/models/'+source,'forecast_day':target.isoformat()}

if forecasts:
    output=spark.createDataFrame(forecasts,'day date,hour int,zone int,estimate double,source string')
    guardar_csv(args.resultados,'city_forecasts.csv',output)
guardar_json(args.resultados,'city_metrics.json',{
    'high_water':high,'previous_high_water':low,'new_event_rows':new_rows,
    'new_input_files':len(input_paths),
    'counts':counts,'training':training,'seconds':timer.segundos,
    'features':variables,'trees':20,'max_depth':8,'max_bins':256,
    'snapshot_path':base,'partitions':events.rdd.getNumPartitions(),
    'policy':'Separate source models. Simulated reports only affect the scenario model. No synthetic accuracy is reported.',
    'limitation':'Partial calendar hours and source coverage can bias counts; forecasts are experimental. No improvement claim without independent future reports.',
    'real_world_accuracy':None})
events.unpersist()
spark.stop()
