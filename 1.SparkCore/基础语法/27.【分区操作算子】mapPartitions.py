# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5],3)

    # 处理分区迭代器对象,需要返回原有的数据类型
    def process(iter):
        result = list()
        for it in iter:
            result.append( it * 10 )
        return result

    # 进行分区迭代器处理
    rdd = rdd.mapPartitions(process)

    # 打印输出,结果为[10, 20, 30, 40, 50]
    print(rdd.collect())