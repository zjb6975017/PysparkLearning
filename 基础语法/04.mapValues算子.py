# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([('a',2),('b',1)])

    # 针对二元元组RDD，对其内部的二元元组的Value进行map操作
    rdd = rdd.mapValues(lambda a : a * 10)

    # 最后打印结果为[('a', 20), ('b', 10)]，即将元组内的value值进行了操作
    print(rdd.collect())