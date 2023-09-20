from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # spark session 객체 생성
    ss: SparkSession = SparkSession.builder\
        .master("local")\
        .appName("wordCount RDD ver")\
        .getOrCreate()

    # spark context 가져오기
    sc: SparkContext = ss.sparkContext

    # load data
    text_file: RDD[str] = sc.textFile("data/words.txt")

    # transformation 연산
    # flatMap 사용 이유, text line 을 가져와서 각자 하나씩 flat 하게 한다
    # eg.
    # text
    # is
    counts = text_file\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda count1, count2: count1 + count2)

    # action
    output = counts.collect()

    for(word, count) in output:
        print(f"{word}:{count}")