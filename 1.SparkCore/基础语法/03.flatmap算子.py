# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd = sc.parallelize(["spark hadoop spark", "hello word hello"])

    # 得到所有的单词，组成rdd
    # 如果用map得到的结果是[['spark', 'hadoop', 'spark'], ['hello', 'word', 'hello']]
    # 如果用flatMap，得到的结果是['spark', 'hadoop', 'spark', 'hello', 'word', 'hello']
    # 注意flatMap的M是大写！！！
    rdd = rdd.flatMap(lambda line:line.split(" "))

    print(rdd.collect())