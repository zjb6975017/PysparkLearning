# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,1,2,2,3,3])

    # 对rdd进行去重
    rdd = rdd.distinct()

    # 最后打印结果为[2, 1, 3] ,去重了，但是不保证顺序
    print(rdd.collect())

    ### 以下列出KV型数据，一样有去重效果
    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd2 = sc.parallelize([('a',1),('a',1),('a',3)])

    # 对rdd进行去重
    rdd2 = rdd2.distinct()

    # 最后打印结果为[('a', 1), ('a', 3)]
    print(rdd2.collect())
