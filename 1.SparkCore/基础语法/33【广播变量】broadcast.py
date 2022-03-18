# coding:utf8

from pyspark import  SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel

'''
需求：将学生分数表中的ID换成名字，名字信息在stu_info_list中
广播变量的作用：
在集群中rdd对象由各个excuter进程运行，该进程可能对应多个线程，如果要使用本地对象，那么每个线程都要取一次，使用广播变量的话每个Excuter只得到一次，线程共享
广播变量的好处：
1）降低网络IO
2）降低Excuter的内存损耗
'''

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    stu_info_list = [(1, '张大仙', 11),
                     (2, '王晓晓', 13),
                     (3, '张甜甜', 11),
                     (4, '王大力', 11)]

    # 1. 将本地Python List对象标记为广播变量
    broadcast_stu_info_list = sc.broadcast(stu_info_list)

    score_info_rdd = sc.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '编程', 99),
        (3, '语文', 99),
        (4, '英语', 99),
        (1, '语文', 99),
        (3, '英语', 99),
        (2, '编程', 99)
    ])

    # 将ID和名字进行对应
    def mapFunc(data):
        id = data[0]
        name = ""
        for stu_info in broadcast_stu_info_list.value:
            stu_id = stu_info[0]
            if  id == stu_id:
                name = stu_info[1]

        return (name,data[1],data[2])

    score_info_rdd = score_info_rdd.map(mapFunc)
    print(score_info_rdd.collect())
