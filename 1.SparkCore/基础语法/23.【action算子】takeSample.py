# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5],3)

    # 从rdd中随机抽样8个，因为第一个参数为允许重复取，所以抽样个数可以大于rdd里的元素个数
    result = rdd.takeSample(True,8)

    # 打印结果为[9,8]
    print(result)


    # 从rdd中随机抽样3个，不允许元素重复。这里如果大于元素个数，那么就会取元素个数作为抽样个数
    result = rdd.takeSample(False,3)
    print(result)
