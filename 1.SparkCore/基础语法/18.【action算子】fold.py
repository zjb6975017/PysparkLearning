# coding:utf8

from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)

    # 使用reduce进行拼接
    result = rdd.fold(10,lambda a,b:a+b)

    # 最后打印结果为85
    # 计算公式是首先这个rdd被分为了三个分区，fold初始值会在分区内聚合
    # 所以分区内的情况是 分区1：10+1+2+3  = 16，分区2：10+4+5+6 = 25，分区3：10+7+8+9 = 34
    # fold初始值也会在分区间聚合，最后的结果为10+16+25+34=85
    print(result)