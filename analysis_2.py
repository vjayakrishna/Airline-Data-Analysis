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

df1_spark = df_spark.select('ORIGIN_AIRPORT', 'MONTH', 'IS_DELAYED').groupBy(['ORIGIN_AIRPORT', 'MONTH'])\
    .agg(F.sum('IS_DELAYED').alias('NUM_DELAYS'), F.count(F.lit(1)).alias('TOTAL'))\
    .orderBy(['ORIGIN_AIRPORT', 'MONTH'])
df1_spark = df1_spark.withColumn('PCT_DELAYS', df1_spark['NUM_DELAYS']/df1_spark['TOTAL']*100)
#df1_spark.show(5)

df2_spark = df1_spark.filter(df1_spark['ORIGIN_AIRPORT'] == "JFK")
#df2_spark.show(5)
df1 = df2_spark.toPandas()
df1.rename(index=str, columns={'PCT_DELAYS': 'JFK Airport'}, inplace=True)
df1.set_index('MONTH', inplace=True)

df2_spark = df1_spark.filter(df1_spark['ORIGIN_AIRPORT'] == "EWR")
#df2_spark.show(5)
df2 = df2_spark.toPandas()
df2.rename(index=str, columns={'PCT_DELAYS': 'Newark Airport'}, inplace=True)
df2.set_index('MONTH', inplace=True)

df2_spark = df1_spark.filter(df1_spark['ORIGIN_AIRPORT'] == "LAX")
#df2_spark.show(5)
df3 = df2_spark.toPandas()
df3.rename(index=str, columns={'PCT_DELAYS': 'Los Angeles-LAX'}, inplace=True)
df3.set_index('MONTH', inplace=True)

df1['JFK Airport'].plot()
df2['Newark Airport'].plot()
df3['Los Angeles-LAX'].plot()
plt.legend(loc=1)
plt.title('Delays at airport locations over time')
plt.xlabel('Month')
plt.xticks(np.arange(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.ylabel('Delay Percentage')
plt.xlim(0, 13)
plt.ylim(0, 100)

plt.show()

