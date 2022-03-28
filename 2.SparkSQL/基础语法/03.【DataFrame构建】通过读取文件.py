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

    # 一、读取text文件数据源，读取特点是将一整行当一列读取，默认列明是value,类型是String
    schema = StructType().add("data",StringType(),nullable=True)
    df = spark.read.format("text").\
        schema(schema = schema).\
        load("测试数据/people.txt")

    # 二、读取json文件数据源，一般不用写.schema，json自带，json带有列名和列类型
    df = spark.read.format("json").\
        load("测试数据/people.json")

    # 三、读取csv文件数据源
    df = spark.read.format("csv").\
        option("sep",";").\
        option("header",False).\
        option("enconding","uft-8").\
        schema("name STRING,age INT,job STRING").\
        load("测试数据/people.csv")

    # 四、读取parquet读数据，parquet内置schema(列名、列类型、是否为空)
    df = spark.read.format("parquet").\
        load("测试数据/users.parquet")