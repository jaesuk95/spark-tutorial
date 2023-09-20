from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions \
    import col, to_timestamp, max, min, mean, date_trunc, collect_set, \
    hour, minute, count


def load_data(ss: SparkSession, from_file, schema):
    if from_file:
        return ss.read.schema(schema).csv("data/log.csv")

    log_data_inmemory = [
        ["130.31.184.234", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["28.252.170.12", "2023-02-26 04:15:21", "GET", "/events", "401", 73],
        ["180.97.92.48", "2023-02-26 04:15:22", "POST", "/parsers", "503", 17],
        ["73.218.61.17", "2023-02-26 04:16:22", "DELETE", "/lists", "201", 91],
        ["24.15.193.50", "2023-02-26 04:17:23", "PUT", "/auth", "400", 24],
    ]

    return ss.createDataFrame(log_data_inmemory, schema)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()

    # define schema
    fields = StructType([
        StructField("ip", StringType(), False),     # is it nullable? true or false
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])

    from_file = True

    df = load_data(ss, from_file, fields)

    # 데이터 확인
    df.show()
    # 스키마 확인
    df.printSchema()


    # a) 컬럼 변환
    # a-1) 현재 latency 컬럼의 단위는 millseconds인데, seconds 단위인
    # latency_seconds 컬럼을 새로 만들기.
    def milliseconds_to_seconds(num):
        return num / 1000


    df = df.withColumn(
        "latency_seconds",
        # milliseconds_to_seconds(col("latency"))
        milliseconds_to_seconds(df.latency)
    )

    df.show()


    # a-2) StringType으로 받은 timestamp 컬럼을, TimestampType으로 변경.
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    df.show()
    df.printSchema()
    # root
    #  |-- ip: string (nullable = true)
    #  |-- timestamp: timestamp (nullable = true) <-- 여기가 변경
    #  |-- method: string (nullable = true)
    #  |-- endpoint: string (nullable = true)
    #  |-- status_code: string (nullable = true)
    #  |-- latency: integer (nullable = true)
    #  |-- latency_seconds: double (nullable = true)


    # b) filter
    # b-1) status_code = 400, endpoint = "/users"인 row만 필터링
    df.filter((df.status_code == "400") & (df.endpoint == "/users"))
    # df.where((df.status_code == "400") & (df.endpoint == "/users"))

# https://github.com/startFromBottom/fc-spark-practices/blob/main/3_sparksql_examples/log_dataframe_ex.py