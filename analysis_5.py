import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def init_spark():
    spark1 = SparkSession.builder.appName("Project").getOrCreate()
    sc1 = spark1.sparkContext
    return spark1, sc1


spark, sc = init_spark()
df_spark = spark.read.format('csv').option('header', 'true').load("files/flights.csv")\
    .select('YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT', 'DEPARTURE_DELAY')
df_spark.fillna(0, subset=['DEPARTURE_DELAY'])
df_spark = df_spark.withColumn('IS_DELAYED', df_spark['DEPARTURE_DELAY'].cast(IntegerType()) > 15)
df_spark = df_spark.withColumn('IS_DELAYED', df_spark['IS_DELAYED'].cast(IntegerType()))
df_spark = df_spark.withColumn('MONTH', df_spark['MONTH'].cast(IntegerType()))

df1_spark = df_spark.filter(df_spark['ORIGIN_AIRPORT'].isin('JFK', 'EWR', 'LAX', 'ORD', 'PHL', 'SAN', 'SFO'))
df1_spark = df1_spark.select('ORIGIN_AIRPORT', 'IS_DELAYED').groupBy(['ORIGIN_AIRPORT'])\
    .agg(F.sum('IS_DELAYED').alias('NUM_DELAYS'), F.count(F.lit(1)).alias('TOTAL'))\
    .orderBy(['ORIGIN_AIRPORT'])
df1_spark = df1_spark.withColumn('PCT_DELAYS', df1_spark['NUM_DELAYS']/df1_spark['TOTAL']*100)
#df1_spark.show(5)

df1 = df1_spark.toPandas()
df1.set_index('ORIGIN_AIRPORT', inplace=True)

df2 = df1.sort_values('PCT_DELAYS', ascending=False)
#print(df2.head())

df2['PCT_DELAYS'].plot.bar(rot=0)
plt.title('Delays at airport locations')
plt.ylabel('Percent of Delays')
plt.xlabel('AIRPORT')
plt.show()
