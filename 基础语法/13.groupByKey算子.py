# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([('a',2),('b',1),('a',3),('c',5),('b',2)])

    # 对rdd进行分组，分组依据为二元元组的key
    rdd = rdd.groupByKey()

    # 最后打印结果为[('b', <pyspark.resultiterable.ResultIterable object at 0x7f01d71559a0>), ('c', <pyspark.resultiterable.ResultIterable object at 0x7f01d7155a30>), ('a', <pyspark.resultiterable.ResultIterable object at 0x7f01d733ddf0>)]
    # 其中返回的元组的valueResultIterable object 是一个可迭代对象
    print(rdd.collect())

    # 我们对可迭代对象的可以做类型转换,转换后，会得到[('b', [1, 2]), ('c', [5]), ('a', [2, 3])]
    print(rdd.map(lambda x: (x[0], list(x[1]))).collect())