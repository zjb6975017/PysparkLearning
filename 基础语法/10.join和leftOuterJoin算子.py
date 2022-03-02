# coding:utf8

from pyspark import  SparkConf,SparkContext

# map 算子 对rdd中每一个分区的的每一条数据都进行操作

if __name__ == '__main__':
    conf = SparkConf().setAppName("TEST").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从序列创建rdd对象，第一个参数是序列，第二个参数是分区个数
    rdd1 = sc.parallelize([(1001,"张三"),(1002,"李四"),(1003,"王五"),(1003,"赵六")])
    rdd2 = sc.parallelize([(1001,"生产部"),(1002,"财务部")])

    # 使用join进行内连接，最后打印的结果为
    # [(1001, ('张三', '生产部')), (1002, ('李四', '财务部'))]
    print(rdd1.join(rdd2).collect())

    # 使用leftOutJoin进行左外连接，最后打印的结果为
    # [(1001, ('张三', '生产部')), (1002, ('李四', '财务部')), (1003, ('王五', None)), (1003, ('赵六', None))]
    print(rdd1.leftOuterJoin(rdd2).collect())

