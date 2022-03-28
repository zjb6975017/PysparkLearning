# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType

if __name__ == '__main__':
    # Spark的入口
    spark= SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 可以使用使用sparkSession对象来生成SC对象
    sc = spark.sparkContext

    rdd = sc.textFile("words.txt").\
        flatMap(lambda x:x.split(" ")).\
        map(lambda x:[x])

    df = rdd.toDF(["word"])

    df.createOrReplaceTempView("words")

    spark.sql("SELECT word,COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()


