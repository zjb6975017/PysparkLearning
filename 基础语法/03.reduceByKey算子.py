# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([('a',2),('b',1),('a',3),('c',5),('b',2)])

    # reduceByKey只针对于KV型的RDD，通过key来进行分组，然后通过func来进行聚合
    # func(v,v) -> v
    rdd = rdd.reduceByKey(lambda a,b: a+b)

    # 最后打印结果为[('b', 3), ('c', 5), ('a', 5)]，所以同key的都相加了
    print(rdd.collect())