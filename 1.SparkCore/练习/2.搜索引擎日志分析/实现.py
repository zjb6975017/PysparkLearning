#coding:utf8

######################################
# 需求：读取输入数据文件，提取北京的的数据，组合北京和商品类别进行输出，同时对结果集进行去重，得到北京售卖的商品类别信息
######################################

from pyspark import  SparkConf,SparkContext
import jieba

if __name__ == '__main__':
    conf = SparkConf().setAppName('Test').setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 读取输入数据
    file_rdd = sc.textFile("输入数据")

    # 对数据进行处理，得到json对象
    split_rdd = file_rdd.map(lambda line: line.split("\t"))

    # 对split_rdd进行缓存
    split_rdd.cache()

    # Todo: 需求1，搜索关键词分析
    # 提取搜索内容
    content_rdd = split_rdd.map(lambda arr:arr[2])
    # 对搜索内容进行分词
    content_cut_rdd = content_rdd.flatMap(lambda x:jieba.cut_for_search(x))
    # 进行分组聚合排序，取出前五名
    result_rdd = content_cut_rdd.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).\
        take(5)

    print(result_rdd)
    split_rdd.unpersist()