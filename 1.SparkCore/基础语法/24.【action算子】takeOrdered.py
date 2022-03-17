# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5],3)

    # 取升序前三个
    result = rdd.takeOrdered(3)
    # 打印结果为5，4，3
    print(result)

    # 取降序前三个，这边的函数只是在排序的时候对数据进行转换，但是对数据本身不会进行更改，
    result = rdd.takeOrdered(3,lambda x: -x)
    # 打印结果为1，2，3
    print(result)
