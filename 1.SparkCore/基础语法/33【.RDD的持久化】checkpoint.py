# coding:utf8

from pyspark import  SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    sc.setCheckpointDir("/ckp")

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd1 = sc.parallelize(["hello word hello hadoop"],3)

    rdd2 = rdd1.flatMap(lambda line:line.split(" "))
    rdd3 = rdd2.map(lambda x:(x,1))

    # 对rdd3进行checkpoint，防止rdd3被使用过多次使用导致重复rdd3的计算，checkpoint集中存储在checkpoint路径
    rdd3.checkpoint()

    # 这里是统计单词出现的个数
    rdd4 = rdd3.reduceByKey(lambda a,b:a + b)
    print(rdd4.collect())

    # 这里也是统计单词出现的个数，使用了两个方法
    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x:sum(x))
    print(rdd6.collect())
