# coding:utf8

from pyspark import  SparkConf,SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd1 = sc.parallelize([1,2,3])
    rdd2 = sc.parallelize([2,3,4])

    # 使用intersection算子求两个rdd的交集
    rdd3 = rdd1.intersection(rdd2)

    # 打印rdd3，打印结果为[2, 3]
    print(rdd3.collect())

