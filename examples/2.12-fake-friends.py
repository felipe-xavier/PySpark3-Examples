from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName('RatingsHistogram').getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("#friends", IntegerType(), True)
])

fake_friends_df = spark.read.csv('examples/datasets/fakefriends.csv',
                                 schema=schema)

fake_friends_df.describe().show()

fake_friends_df.show()

avg_by_age_df = fake_friends_df\
    .groupby('age')\
    .agg(F.mean('#friends'), F.count('age'))\
    .orderBy('age')\
    .cache()

avg_by_age_df.show(10)

