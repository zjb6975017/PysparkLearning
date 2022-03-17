# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([('a',2),('B',1),('a',3),('n',5),('p',2)],3)

    # 根据value来进行排序
    '''
    参数1:True表示升序，False表示降序
    参数2:排序的分区数
    参数3:keyfunc为在排序前对key做预处理，比如大写改成小写
    '''
    '''注意！！如果要全局有序，排序分区数请设置为1'''
    rdd = rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower())
    # 最后打印结果为[('b', 1), ('a', 2), ('p', 2), ('a', 3), ('n', 5)]
    print(rdd.collect())