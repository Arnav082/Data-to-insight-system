from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_timestamp, to_date, avg, count, trim

spark = SparkSession.builder.appName("taxi etl").getOrCreate()

df = spark.read.option('header',True).csv('/data/raw/yellow_tripdata_2016-01.csv')


df = df.withColumn('fare_amount',trim(col('fare_amount')))
df = df.withColumn('pickup_datetime', trim(col('pickup_datetime')))

df = df.withColumn("fare_amount", col("fare_amount").cast("double"))

df = df.withColumn(
    "pickup_ts",
    to_timestamp("pickup_datetime", "M/d/yyyy H:mm")
)

df = df.dropna(subset=['fare_amount','pickup_ts'])

df = df.filter(
    (col("fare_amount") >= 0.1) &
    (col("fare_amount") <= 500)
)

df = df.withColumn("pickup_date", to_date("pickup_ts"))

df = df.filter(col("pickup_date").isNotNull())

df.write.mode('overwrite').format('parquet').partitionBy('pickup_date').save('/data/silver/taxi')

gold_df = df.groupBy('pickup_date') \
            .agg(
                avg('fare_amount').alias('avg_fare'),
                count('*').alias('total_trips')
            )

gold_df.write.mode('overwrite').format('parquet').save('/data/gold/taxi_kpis')

spark.stop()