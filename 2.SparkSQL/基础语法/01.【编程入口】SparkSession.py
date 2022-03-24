# coding:utf8

from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Spark的入口
    spark= SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 可以使用使用sparkSession对象来生成SC对象
    sc = spark.sparkContext

    # 读取学生分数表
    df = spark.read.csv("测试数据/stu_score.txt",sep = ',',header=False)

    # 转换成dataframe
    df2 = df.toDF("id","name","score")

    # 打印表结构，打印结果如下：
    # root
    # | -- id: string(nullable=true)
    # | -- name: string(nullable=true)
    # | -- score: string(nullable=true)
    df2.printSchema()

    # 打印表数据如下
    # +---+----+-----+
    # | id | name | score |
    # +---+----+-----+
    # | 1 | 语文 | 99 |
    # | 2 | 语文 | 99 |
    # | 3 | 语文 | 99 |
    df2.show()