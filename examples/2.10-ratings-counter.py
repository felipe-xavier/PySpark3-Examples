from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType


spark = SparkSession.builder.appName('RatingsHistogram').getOrCreate()
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])
ratings_df = spark.read.csv('datasets/u.data',
                            sep='\t',
                            schema=schema)

ratings_df.printSchema()
ratings_df.show(5)
# +-------+--------+------+---------+
# |user_id|movie_id|rating|timestamp|
# +-------+--------+------+---------+
# |    196|     242|     3|881250949|
# |    186|     302|     3|891717742|
# |     22|     377|     1|878887116|
# |    244|      51|     2|880606923|
# |    166|     346|     1|886397596|
# +-------+--------+------+---------+

hist_df = ratings_df\
    .groupby('rating')\
    .count()\
    .orderBy('count', ascending=False)\
    .cache()

hist_df.show()
# +------+-----+
# |rating|count|
# +------+-----+
# |     4|34174|
# |     3|27145|
# |     5|21201|
# |     2|11370|
# |     1| 6110|
# +------+-----+