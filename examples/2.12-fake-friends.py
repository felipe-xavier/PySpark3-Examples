from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName('FakeFriends').getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("#friends", IntegerType(), True)
])

fake_friends_df = spark.read.csv('datasets/fakefriends.csv',
                                 schema=schema)

fake_friends_df.describe().show()

fake_friends_df.show(5)
# +---+--------+---+--------+
# | id|    name|age|#friends|
# +---+--------+---+--------+
# |  0|    Will| 33|     385|
# |  1|Jean-Luc| 26|       2|
# |  2|    Hugh| 55|     221|
# |  3|  Deanna| 40|     465|
# |  4|   Quark| 68|      21|

avg_by_age_df = fake_friends_df\
    .groupby('age')\
    .agg(F.mean('#friends'), F.count('age'))\
    .orderBy('age')\
    .cache()

avg_by_age_df.show(5)
# +---+------------------+----------+
# |age|     avg(#friends)|count(age)|
# +---+------------------+----------+
# | 18|           343.375|         8|
# | 19|213.27272727272728|        11|
# | 20|             165.0|         5|
# | 21|           350.875|         8|
# | 22|206.42857142857142|         7|
# +---+------------------+----------+
