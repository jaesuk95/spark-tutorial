from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("Streaming dataframe example") \
        .getOrCreate()

    # schema
    schemas = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])


    def hello_streaming():
        lines = ss.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345) \
            .load()
        # to test goto cli 'nc -lk 12345'

        # transformation

        # write
        lines.writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(processingTime="2 seconds") \
            .start() \
            .awaitTermination()


    # hello_streaming()

    # 일반 실습에서는 socket 이 아닌 kafka 를 많이 사용한다

    def read_from_socket():
        lines = ss.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345) \
            .load()

        cols = ["ip", "timestamp", "method", "endpoint", "status_code", "latency"]
        df = lines \
            .withColumn("ip", F.split(lines["value"], ",").getItem(0)) \
            .withColumn("timestamp", F.split(lines["value"], ",").getItem(1)) \
            .withColumn("method", F.split(lines["value"], ",").getItem(2)) \
            .withColumn("endpoint", F.split(lines["value"], ",").getItem(3)) \
            .withColumn("status_code", F.split(lines["value"], ",").getItem(4)) \
            .withColumn("latency", F.split(lines["value"], ",").getItem(5)).select(cols)

        # filter : status_code = 400, endpoint "/users"
        df = df.filter((df.status_code == "400") & (df.endpoint == "/users"))

        # groupBy , aggregation : method, endpoint 별 latency 의 최대, 최소 평균
        # group_cols = ["method", "endpoint"]
        # df = df.groupby(group_cols) \
        #     .agg(F.max("latency").alias("max_latency"),
        #          F.min("latency").alias("min_latency"),
        #          F.mean("latency").alias("mean_latency"))

        df.writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(processingTime="2 seconds") \
            .start() \
            .awaitTermination()

        # read_from_socket()


    def read_from_file():
        logs_df = ss.readStream \
            .format("csv") \
            .option("header", "false") \
            .schema(schema=schemas) \
            .load("data/log/")

        logs_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()


    # read_from_socket()
    read_from_file()
