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
    .select('YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'DEPARTURE_DELAY')
df_spark = df_spark.withColumn('ROUTE', F.concat(F.col('ORIGIN_AIRPORT'), F.lit('-'), F.col('DESTINATION_AIRPORT')))
#df_spark.show(5)

df1_spark = df_spark.select('ROUTE').groupBy(['ROUTE'])\
    .agg(F.count(F.lit(1)).alias('COUNT'))\
    .orderBy(['COUNT'], ascending=False)
#df1_spark.show(5)

df1 = df1_spark.toPandas().head(5)
df1.set_index('ROUTE', inplace=True)
print(df1.head(5))

df1.plot.bar(y='COUNT', rot=0, width=0.25)
plt.title('Most popular routes')
plt.ylabel('Total # flights in the year')
plt.show()

