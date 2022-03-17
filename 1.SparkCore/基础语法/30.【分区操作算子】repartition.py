# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5],3)

    # 打印初始分区个数,结果为3
    print(rdd.getNumPartitions())

    # 进行分区重新分布
    rdd = rdd.repartition(5)

    # 打印初始分区个数，结果为5
    print(rdd.getNumPartitions())

