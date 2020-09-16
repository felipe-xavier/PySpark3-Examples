from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
import codecs


def loadMovieNames():
    movie_names = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open('datasets/u.item', 'r',
                     encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

# Broadcast a dictionary
name_dict = spark.sparkContext.broadcast(loadMovieNames())

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
    .sort('count', ascending=False)

ratings_df.show(5)
# +--------+-----+
# |movie_id|count|
# +--------+-----+
# |      50|  583|
# |     258|  509|
# |     100|  508|
# |     181|  507|
# |     294|  485|
# +--------+-----+

def lookup_name(movie_id):
    return name_dict.value[movie_id]

lookup_name_udf = F.udf(lookup_name, StringType())

named_movies_df = ratings_df.withColumn('name', lookup_name_udf('movie_id'))

named_movies_df.show(5)