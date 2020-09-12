from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RatingsHistogram').getOrCreate()

ratings_df = spark.read.csv('/data/courses/SparkCourse/ml-100k/u.data', sep='\t')

print(ratings_df.printSchema())
ratings_df.show()

hist_df = ratings_df\
    .groupby('_c2')\
    .count()\
    .orderBy('count', ascending=False)\
    .cache()

print(hist_df.show())