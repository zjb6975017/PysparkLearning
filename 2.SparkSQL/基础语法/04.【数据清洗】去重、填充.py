# coding:utf8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType

if __name__ == '__main__':
    # Spark的入口
    spark= SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 读取数据
    df = spark.read.format("csv").\
        option("sep",";").\
        option("header",False).\
        option("enconding","uft-8").\
        schema("name STRING,age INT,job STRING").\
        load("测试数据/people.csv")

    # TODO 1:去除重复项
    # 去除重复项,不带参数代表所有列重复才算重复
    df.dropDuplicates().show()
    # 去除重复项，代表name和age两列重复就算重复
    df.dropDuplicates(['name', 'age']).show()

    # TODO 2:删除空值行
    # 去除带有空值的行，不带参数使用代表只要任意一列有空值，那么就删除该行数据
    df.dropna().show()

    # 至少有3个有效列，才能够保留,否则删除该行数据
    df.dropna(thresh =3).show()

    # 至少有2个有效列，并且name和age列不能为空
    df.dropna(thresh=2, subset=["name","age"]).show()

    # TODO 3：填充空值行
    # 所有空值填充
    df.fillna("loss").show()

    # 指定列填充
    df.fillna("loss",["name"]).show()

    # 多个列填充，并提供填充规则
    df.fillna({"name":"未知姓名","age":1})