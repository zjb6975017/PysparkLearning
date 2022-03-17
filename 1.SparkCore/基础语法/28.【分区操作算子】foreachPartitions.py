# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5],3)

    # 处理分区迭代器对象,需要返回原有的数据类型
    def process(iter):
        for it in iter:
            print(it * 10)

    # 进行分区迭代器处理
    rdd.foreachPartition(process)
