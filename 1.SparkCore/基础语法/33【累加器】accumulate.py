# coding:utf8

from pyspark import  SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel

'''
需求：统计map中的元素数量，使用累加器
广播变量的作用：
由于代码在Excuter中执行，不使用累加器的话那么Driver将得不到统计结果
'''

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 将序列分为两个分区
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)

    aclmt_num = sc.accumulator(0)

    def mapFunc(data):
        global aclmt_num
        aclmt_num += 1

    rdd2 = rdd.map(mapFunc)

    # 下面是避免累加器陷阱的代码
    #rdd.cache()

    rdd2.collect()
    # 打印结果为10
    print(aclmt_num)

    # 累加器陷阱
    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    # 打印结果为20，因为rdd2在collect被释放了，但是累加器变量还在，所以rdd2又被构造了一次，导致mapFunc一共被调用两次，可以用cache来避免累加器陷阱
    print(aclmt_num)