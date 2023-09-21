from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Streaming context
conf = SparkConf().setAppName("KafkaToMySQL")
ssc = StreamingContext(conf, 5)  # 5-second batch interval

# Create a Kafka stream
kafkaParams = {"bootstrap.servers": "your_kafka_broker",
               "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
               "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
               "group.id": "your_group_id"}
topics = ["your_kafka_topic"]
kafkaStream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)

# Define a schema for the data
schema = StructType([StructField("userId", StringType(), True),
                     StructField("data", StringType(), True)])

# Function to update or insert data into MySQL
def update_or_insert_data(data, mysqlURL, mysqlTable):
    data.write.mode("overwrite").jdbc(mysqlURL, mysqlTable, properties={})

# Process each batch of data
def process_rdd(rdd):
    spark = SparkSession.builder.config(conf=rdd.context.getConf()).getOrCreate()

    # Convert the RDD to a DataFrame
    data = spark.read.schema(schema).json(rdd.map(lambda x: x[1]))

    # Check for existing userId data in MySQL
    mysqlURL = "jdbc:mysql://your_mysql_host:3306/your_database"
    mysqlTable = "your_mysql_table"

    existingData = spark.read.jdbc(mysqlURL, mysqlTable, properties={}).select("userId")

    # Identify new and existing data
    newData = data.join(existingData, "userId", "leftanti")
    updatedData = data.join(existingData, "userId", "inner")

    # Update existing data in MySQL
    if not updatedData.isEmpty():
        update_or_insert_data(updatedData, mysqlURL, mysqlTable)

    # Insert new data into MySQL
    if not newData.isEmpty():
        update_or_insert_data(newData, mysqlURL, mysqlTable)

# Apply the processing function to each batch
kafkaStream.foreachRDD(process_rdd)

# Start the Spark Streaming context
ssc.start()
ssc.awaitTermination()
