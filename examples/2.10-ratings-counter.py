from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType


spark = SparkSession.builder.appName('RatingsHistogram').getOrCreate()
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])
ratings_df = spark.read.csv('/data/courses/SparkCourse/ml-100k/u.data',
                            sep='\t',
                            schema=schema)

ratings_df.printSchema()
ratings_df.show()

hist_df = ratings_df\
    .groupby('rating')\
    .count()\
    .orderBy('count', ascending=False)\
    .cache()

hist_df.show()