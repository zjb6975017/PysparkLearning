# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd1 = sc.parallelize([1,2,3,4,5,6],3)

    # 从文件中创建rdd对象，第一个参数是文件名，第二个参数是分区个数
    # 第二个参数是尽力而为，而不是一定会到
    rdd2 = sc.textFile("/tmp/testfile", 3)
    rdd3 = sc.textFile("/tmp/testfile")  # 也可以不用参数

    # 从小文件中创建rdd对象，这里返回的是一个元组，前一个是文件名，后一个是文件类型
    # 类似《[('file:/tmp/testfile', 'hello word hello hadoop\n')]》
    rdd4 = sc.wholeTextFiles("/tmp/testfile", 3)
    rdd5 = sc.wholeTextFiles("/tmp/testfile") #也可以不用参数
