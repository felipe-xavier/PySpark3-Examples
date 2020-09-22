from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
import codecs


# def load_hero_names():
#     hero_names = {}
#     # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
#     with codecs.open('datasets/Marvel-names.txt', 'r',
#                      encoding='ISO-8859-1', errors='ignore') as f:
#         for line in f:
#             line = line.replace('"', '')
#             fields = line.split(' ')
#             hero_names[int(fields[0])] = ' '.join(fields[1:])
#     return hero_names

spark = SparkSession.builder.appName('PopularHeroes').getOrCreate()

# Broadcast a dictionary
# name_dict = spark.sparkContext.broadcast(load_hero_names())

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])
hero_names = spark.read.csv('datasets/Marvel-names.txt',
                            sep=' ',
                            schema=schema)
#
# hero_names.show(5)
# +---+--------------------+
# | id|                name|
# +---+--------------------+
# |  1|24-HOUR MAN/EMMANUEL|
# |  2|3-D MAN/CHARLES CHAN|
# |  3|    4-D MAN/MERCURIO|
# |  4|             8-BALL/|
# |  5|                   A|
# +---+--------------------+

lines = spark.read.text('datasets/Marvel-graph.txt')
# lines.show(5)

connections = lines.withColumn('id', F.split(F.col('value'), ' ')[0])\
    .withColumn('connections', F.size(F.split(F.col('value'), ' ')) - 1)\
    .groupby('id')\
    .agg(F.sum('connections').alias('connections'))\
    .sort('connections', ascending=False)\

most_popular = connections.first()
most_popular_name = hero_names.filter(F.col('id') == most_popular.id).select('name').first()
print(most_popular_name.name)

# ratings_df = ratings_df\
#     .groupby('movie_id')\
#     .count()\
#     .sort('count', ascending=False)
#
# ratings_df.show(5)
# +--------+-----+
# |movie_id|count|
# +--------+-----+
# |      50|  583|
# |     258|  509|
# |     100|  508|
# |     181|  507|
# |     294|  485|
# +--------+-----+

# def lookup_name(movie_id):
#     return name_dict.value.get(movie_id)
#
# lookup_name_udf = F.udf(lookup_name, StringType())
#
# named_movies_df = ratings_df.withColumn('name', lookup_name_udf('movie_id'))
#
# named_movies_df.show(5)