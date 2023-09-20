import pyspark.sql.functions as f
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # spark session 객체 생성
    ss: SparkSession = SparkSession.builder\
        .master("local")\
        .appName("wordCount RDD ver")\
        .getOrCreate()

    # load data
    data_frame = ss.read.text("data/words.txt")

    # transformation
    data_frame = data_frame.withColumn('word', f.explode(f.split(f.col('value'), ' ')))\
        .withColumn("count", f.lit(1)).groupby("word").sum()

    data_frame.show()