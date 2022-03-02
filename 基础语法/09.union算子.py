# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd1 = sc.parallelize([1,1,2,2,3,3])
    rdd2 = sc.parallelize(['a','b','c'])

    # 将rdd1和rdd2合并成rdd3
    rdd3 = rdd1.union(rdd2)

    # 打印rdd3，最后结果为[1, 1, 2, 2, 3, 3, 'a', 'b', 'c']
    print(rdd3.collect())

    '''
    union算子有两个特性
    1）没有进行去重
    2）可以合并不同类型的rdd
    '''



