# coding:utf8

from pyspark import  SparkConf,SparkContext


if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)

    # 使用glom进行分区嵌套以此来查看数据的分区排布
    rdd = rdd.glom()
    # 最后打印结果为[[1, 2, 3], [4, 5, 6], [7, 8, 9]]，因为分成了3个分区
    print(rdd.collect())

    # 技巧：使用flatMap可以对glom后的数据进行分区解嵌套
    rdd = rdd.flatMap(lambda x:x)
    # 最后打印结果为[1, 2, 3, 4, 5, 6, 7, 8, 9]
    print(rdd.collect())

