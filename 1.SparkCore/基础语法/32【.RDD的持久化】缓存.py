# coding:utf8

from pyspark import  SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd1 = sc.parallelize(["hello word hello hadoop"],3)

    rdd2 = rdd1.flatMap(lambda line:line.split(" "))
    rdd3 = rdd2.map(lambda x:(x,1))

    # 对rdd3进行缓存，防止rdd3被使用过多次使用导致重复rdd3的计算，缓存是在各个excuter中分散存储的
    rdd3.cache()
    '''
    除了cache，还可以使用下面进行RDD缓存的精细化控制
    rdd3.persist(storageLevel=StorageLevel.MEMORY_ONLY)   # 仅内存缓存
    rdd3.persist(storageLevel=StorageLevel.MEMORY_ONLY_2)  # 仅内存缓存,2个副本
    rdd3.persist(storageLevel=StorageLevel.DISK_ONLY)  # 仅缓存到硬盘上
    rdd3.persist(storageLevel=StorageLevel.DISK_ONLY_2)  # 仅缓存到硬盘上,2个副本
    rdd3.persist(storageLevel=StorageLevel.DISK_ONLY_3)  # 仅缓存到硬盘上,3个副本
    rdd3.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)  # 先放内存，不够放硬盘，这个常用
    rdd3.persist(storageLevel=StorageLevel.MEMORY_AND_DISK_2)  # 先放内存，不够放硬盘，2个副本
    rdd3.persist(storageLevel=StorageLevel.OFF_HEAP)  # 堆外内存（系统内存）
    '''
    # 这里是统计单词出现的个数
    rdd4 = rdd3.reduceByKey(lambda a,b:a + b)
    print(rdd4.collect())

    # 这里也是统计单词出现的个数，使用了两个方法
    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x:sum(x))
    print(rdd6.collect())

    rdd3.unpersist()