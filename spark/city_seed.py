"""Derive a bounded generator seed from actual cleaned historical trips."""
import argparse
import datetime
from pyspark.sql import functions as F
from comun import sesion,HDFS,guardar_json

p=argparse.ArgumentParser()
p.add_argument('--resultados',required=True)
args=p.parse_args()
spark=sesion('City historical seed')
trips=spark.read.parquet(HDFS+'/limpio/viajes')
last=trips.agg(F.max('fecha')).first()[0]
sample=(trips.select(F.col('zona_origen').alias('origin'),F.col('zona_destino').alias('destination'),
                     F.col('duracion_min').alias('duration_min'),F.col('distancia_mi').alias('distance_mi'),
                     F.col('tarifa').alias('fare'),F.col('dia_semana').alias('weekday'),'hora')
         .sample(False,.002,42).orderBy(F.rand(42)).limit(10000).withColumnRenamed('hora','hour'))
guardar_json(args.resultados,'city_seed.json',{
    'source':'NYC TLC cleaned completed trips through '+last.isoformat(),
    'suggested_start':(last+datetime.timedelta(days=1)).isoformat()+'T00:00:00',
    'sampling':'seeded Bernoulli sample capped at 10000; not a complete city population',
    'trips':[r.asDict() for r in sample.collect()]})
spark.stop()
