#coding:utf8

######################################
# 需求：读取输入数据文件，提取北京的的数据，组合北京和商品类别进行输出，同时对结果集进行去重，得到北京售卖的商品类别信息
######################################

from pyspark import  SparkConf,SparkContext
import json

if __name__ == '__main__':
    conf = SparkConf().setAppName('Test').setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 读取输入数据
    file_rdd = sc.textFile("输入数据")

    # 对数据进行处理，得到json对象
    jsons_rdd = file_rdd.flatMap(lambda line: line.split("|"))

    # 由于json对象操作复杂，使用json库转换为python的字典对象
    dict_rdd = jsons_rdd.map(lambda json_str:json.loads(json_str))

    # 通过filter来筛选rdd,筛选出北京地区情况
    beijing_rdd = dict_rdd.filter(lambda d:d['areaName'] == '北京')

    # 通过map进行组合
    beijing_rdd = beijing_rdd.map(lambda d:d['areaName'] + "_" + d['category'])

    # 进行去重
    result_rdd = beijing_rdd.distinct()

    print(result_rdd.collect())