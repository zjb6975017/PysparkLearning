# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize(['a','a','b','c'])

    # 自己构造元组,最后输出会是[('a',1),('a',1),('b',1),('c',1)]
    rdd = rdd.map(lambda x:(x,1))

    # 统计key的个数
    result = rdd.countByKey()

    # 最后打印结果为是一个字段：defaultdict(<class 'int'>, {'a': 2, 'b': 1, 'c': 1})
    print(result)

    # 可以加上key得到具体值，最后结果是2
    print(result['a'])