# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5,6])

    # 过滤rdd，只保留基数
    rdd = rdd.filter(lambda x: x % 2 ==1 )

    # 最后打印结果为[1, 3, 5]
    print(rdd.collect())
