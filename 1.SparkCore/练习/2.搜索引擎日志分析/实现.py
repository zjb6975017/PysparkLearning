#coding:utf8

######################################
# 需求：读取输入数据文件，提取北京的的数据，组合北京和商品类别进行输出，同时对结果集进行去重，得到北京售卖的商品类别信息
######################################

from pyspark import  SparkConf,SparkContext
import jieba
from operator import add

if __name__ == '__main__':
    conf = SparkConf().setAppName('Test').setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 读取输入数据
    file_rdd = sc.textFile("输入数据")

    # 对数据进行处理，得到json对象
    split_rdd = file_rdd.map(lambda line: line.split("\t"))

    # 对split_rdd进行缓存
    split_rdd.cache()

    # # Todo: 需求1，搜索关键词分析
    # # 提取搜索内容
    # content_rdd = split_rdd.map(lambda arr:arr[2])
    # # 对搜索内容进行分词
    # content_cut_rdd = content_rdd.flatMap(lambda x:jieba.cut_for_search(x))
    # # 进行分组聚合排序，取出前五名
    # result_rdd = content_cut_rdd.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).\
    #     sortBy(lambda x:x[1],ascending=False,numPartitions=1).\
    #     take(5)
    #
    # print(result_rdd)


    # # Todo: 需求2，用户组合分析
    # # 取出用户ID和用户搜索关键词
    # user_content_rdd = split_rdd.map(lambda arr:(arr[1],arr[2]))
    #
    # def extract_user_and_words(data):
    #     user_id = data[0]
    #     content = data[1]
    #     words = jieba.cut_for_search(content)
    #     result = list()
    #     for word in words:
    #         result.append((user_id + "_" + word,1))
    #     return result
    #
    # user_content_rdd = user_content_rdd.flatMap(extract_user_and_words)
    # result = user_content_rdd.reduceByKey(lambda a,b:a+b).\
    #     sortBy(lambda x:x[1],ascending=False,numPartitions=1).\
    #     take(5)
    #
    # print(result)

    # Todo: 需求3，用户时间段分析
    # 取出小时数据
    user_time = split_rdd.map(lambda x:x[0])
    # 数据整合成数组
    user_time = user_time.map(lambda x:(x.split(":")[0],1))

    # 分组、聚合、计算，这里是用了operater包里的add方法
    result  = user_time.reduceByKey(add).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).\
        collect()

    print(result)

    split_rdd.unpersist()