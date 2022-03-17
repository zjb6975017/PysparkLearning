# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([('a',1),('b',1),('c',1),('d',1),('e',1)],3)

    # 处理分区迭代器对象,需要返回原有的数据类型
    def process(key):
        if key == 'a' : return 0
        if key == 'b': return 1
        return 2

    # 进行重新分区，partitionBy是转换算子，只能对kv型的rdd做重新分区
    rdd = rdd.partitionBy(3,process)

    # 最后的打印结果为[[('a', 1)], [('b', 1)], [('c', 1), ('d', 1), ('e', 1)]]
    print(rdd.glom().collect())

