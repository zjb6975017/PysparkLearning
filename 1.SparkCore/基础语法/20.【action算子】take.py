# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)

    # 取出rdd中的前2个元素
    result = rdd.take(2)

    # 打印结果为[1,2]
    print(result)