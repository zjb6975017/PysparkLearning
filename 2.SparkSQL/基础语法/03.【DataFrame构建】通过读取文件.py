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
    rdd = sc.textFile("测试数据/stu_score.txt")

    rdd = rdd.map(lambda x:x.split(",")).\
        map(lambda x:(int(x[0]),x[1],int(x[2])))

    # 通过crateDataFrame函数构造DataFrame
    df = spark.createDataFrame(rdd,["id","name","score"])

    # 打印表结构
    df.printSchema()

    # 打印表内容
    df.show()