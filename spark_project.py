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
df_spark.show(5)
print('Total # records in the dataset:', df_spark.count())

df1_spark = df_spark.select('AIRLINE', 'MONTH', 'IS_DELAYED').groupBy(['AIRLINE', 'MONTH'])\
    .agg(F.sum('IS_DELAYED').alias('NUM_DELAYS'), F.count(F.lit(1)).alias('TOTAL'))\
    .orderBy(['AIRLINE', 'MONTH'])
df1_spark = df1_spark.withColumn('PCT_DELAYS', df1_spark['NUM_DELAYS']/df1_spark['TOTAL']*100)
#df1_spark.show(5)

df2_spark = df1_spark.filter(df1_spark['AIRLINE'] == "AA")
df1 = df2_spark.toPandas()
df1.rename(index=str, columns={'PCT_DELAYS': 'American Airlines'}, inplace=True)
df1.set_index('MONTH', inplace=True)
#print(df1.head())

df2_spark = df1_spark.filter(df1_spark['AIRLINE'] == "DL")
#df2_spark.show(5)
df2 = df2_spark.toPandas()
df2.rename(index=str, columns={'PCT_DELAYS': 'Delta Airlines'}, inplace=True)
df2.set_index('MONTH', inplace=True)
#print(df2.head())

df2_spark = df1_spark.filter(df1_spark['AIRLINE'] == "UA")
df3 = df2_spark.toPandas()
df3.rename(index=str, columns={'PCT_DELAYS': 'United Airlines'}, inplace=True)
df3.set_index('MONTH', inplace=True)
#print(df3.head())

df2_spark = df1_spark.filter(df1_spark['AIRLINE'] == "HA")
df4 = df2_spark.toPandas()
df4.rename(index=str, columns={'PCT_DELAYS': 'Hawaiian Airlines'}, inplace=True)
df4.set_index('MONTH', inplace=True)
#print(df4.head())

df1['American Airlines'].plot()
df2['Delta Airlines'].plot()
df3['United Airlines'].plot()
df4['Hawaiian Airlines'].plot()
plt.legend(loc=1)
plt.title('Airline Delays over time')
plt.xlabel('Month')
plt.xticks(np.arange(1, 13),
           ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.ylabel('Delay Percentage')
plt.xlim(0, 13)
plt.ylim(0, 100)
plt.show()

