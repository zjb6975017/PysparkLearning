#coding:utf8

######################################
# 需求：读取输入数据文件，提取北京的的数据，组合北京和商品类别进行输出，同时对结果集进行去重，得到北京售卖的商品类别信息
######################################

from pyspark import  SparkConf,SparkContext
from operator import add
import re

if __name__ == '__main__':
    conf = SparkConf().setAppName('Test').setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 特殊字符的list定义
    abnormal_char = [",", ".", "!", "#", "$", "%"]

    # 定义广播变量
    broacast_abnormal_char = sc.broadcast(abnormal_char)

    # 定义累加器
    acmlt = sc.accumulator(0)

    # 读取输入数据
    file_rdd = sc.textFile("输入数据")

    # 对数据进行处理，过滤空行
    line_rdd = file_rdd.filter(lambda line: line.strip())

    # 过滤前后空格
    line_rdd = line_rdd.map(lambda line:line.strip())

    # 进行切分,由于单词中间空格数量不一定，所以使用正则进行切分
    words_rdd = line_rdd.flatMap(lambda line:re.split("\s+",line))

    # 过滤出正常单词，在过滤中统计特殊字符的数量
    def abnormal_word_filter_func(word):
        global acmlt
        isNormalChar = True
        abnormal_char = broacast_abnormal_char.value
        if word in abnormal_char:
            # 表示这个字符是特殊字符
            acmlt += 1
            return False
        else:
            return True

    # 过滤出正常单词
    normal_words_rdd = words_rdd.filter(abnormal_word_filter_func)

    # 统计正常单词情况
    result_rdd = normal_words_rdd.map(lambda x:(x,1)).reduceByKey(add)

    # 打印结果
    print(result_rdd.collect())

    # 打印结果，这个要放在collect之后，要不然RDD代码都没有执行
    print("特殊字符：",acmlt)