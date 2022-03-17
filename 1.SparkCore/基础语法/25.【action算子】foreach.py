# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5],3)

    # foreach不返回结果，跟其他算子不同的是foreach是在每个excuter直接执行完，不用汇聚到driver进行处理，特定场景下效率更高
    result = rdd.foreach(lambda x:print(x*10))
    # 打印结果为
    # 20
    # 30
    # 10
    # 40
    # 50
    print(result)
