# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize([1,2,3,4,5,6],3)

    # 第一种使用方法，定义函数，在map中传入函数名
    def add(data):
        return  data * 10
    print(rdd.map(add).collect())

    # 第二种使用方法，使用lambda表达式，传入匿名函数
    print(rdd.map(lambda x: x * 10).collect())