# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([('a',2),('b',1),('a',3),('n',5),('p',2)],3)

    # 根据value来进行排序
    '''
    参数1:函数，表示的是告诉spark按照数据的那个列进行排序
    参数2:True表示升序，False表示降序
    参数3:排序的分区数
    '''
    '''注意！！如果要全局有序，排序分区数请设置为1'''
    rdd = rdd.sortBy(lambda x:x[1],ascending=True,numPartitions=3)
    # 最后打印结果为[('b', 1), ('a', 2), ('p', 2), ('a', 3), ('n', 5)]
    print(rdd.collect())

    # 根据Key来排序
    rdd = rdd.sortBy(lambda x:x[0],ascending=False,numPartitions=1)

    # 最后打印结果为[('p', 2), ('n', 5), ('b', 1), ('a', 2), ('a', 3)]
    print(rdd.collect())