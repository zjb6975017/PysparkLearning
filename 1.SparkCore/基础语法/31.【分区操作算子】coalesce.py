# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5],3)

    # 打印初始分区个数,结果为3
    print(rdd.getNumPartitions())

    # 打印初始分区个数，结果为3,因为coalesce将分区数增加的时候，必须要把shuffle=True给加上
    print(rdd.coalesce(5).getNumPartitions())
    
    # 结果为5，分区重新排布成功
    print(rdd.coalesce(5,shuffle=True).getNumPartitions())