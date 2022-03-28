from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType

if __name__ == '__main__':
    # Spark的入口
    spark= SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    schema = StructType().add("user_id",StringType(),nullable=True).\
        add("movie_id",StringType(),nullable=True).\
        add("rank",IntegerType(),nullable=True).\
        add("ts",StringType(),nullable=True)

    # 读取数据源
    df = spark.read.format("csv").\
        option("sep","\t").\
        schema(schema=schema).\
        load("u.data")

    df.createOrReplaceTempView("tb_movie")

    # TODO 1:查询用户平均分
    spark.sql("select user_id,round(avg(rank),2) as avg_user from tb_movie group by user_id order by avg_user desc").show()

    # TODO 2:查询电影平均分
    spark.sql("select movie_id,round(avg(rank),2) as avg_movie from tb_movie group by movie_id order by avg_movie desc").show()

    # TODO 3:查询大于平均分的电影的数量
    # 先得到电影平均分,通过first得到row对象，然后取出平均分
    avg_movie = spark.sql("select avg(rank) as avg_movie from tb_movie").first()['avg_movie']

    # 通过format方法给sql传递变量
    spark.sql("select count(movie_id) as sum_movie from tb_movie where rank > {}".format(avg_movie)).show()

    # TODO 4:查询高分电影中(>3)打分次数最多的用户，并求出此人打的平均分
    most_rank_user_id = spark.sql("select user_id,count(user_id) as rank_count from tb_movie where rank > 3 group by user_id order by rank_count desc").first()["user_id"]
    spark.sql("select round(avg(rank),2) from tb_movie where user_id ={}".format(most_rank_user_id)).show()

    # TODO 5: 查询每个用户的平均打分，最低打分，最高打分
    spark.sql("select user_id,round(avg(rank),2),round(max(rank),2),round(min(rank),2) from tb_movie group by user_id").show()

    # TODO 6:查询被打分超过100次的电影,并取这些电影的平均分的排名TOP10
    # 查询打分超过100次的电影
    spark.sql("select movie_id,count(movie_id) as cnt,round(avg(rank)) as avg_rank from tb_movie group by movie_id having  cnt > 100 order by avg_rank desc limit 10").show()