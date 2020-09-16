from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType


spark = SparkSession.builder.appName('PopularMovies').getOrCreate()
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])
ratings_df = spark.read.csv('datasets/u.data',
                            sep='\t',
                            schema=schema)
ratings_df = ratings_df.select(['movie_id', 'rating'])

ratings_df.show(5)
# +--------+------+
# |movie_id|rating|
# +--------+------+
# |     242|     3|
# |     302|     3|
# |     377|     1|
# |      51|     2|
# |     346|     1|
# +--------+------+

ratings_df = ratings_df\
    .groupby('movie_id')\
    .count()\
    .sort('count', ascending=False)\
    .show(5)
# +--------+-----+
# |movie_id|count|
# +--------+-----+
# |      50|  583|
# |     258|  509|
# |     100|  508|
# |     181|  507|
# |     294|  485|
# +--------+-----+