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

hero_names.show(5)
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

connections = lines.withColumn('id', F.split(F.col('value'), ' ')[0])\
    .withColumn('connections', F.size(F.split(F.col('value'), ' ')) - 1)\
    .groupby('id')\
    .agg(F.sum('connections').alias('connections'))\
    .sort('connections', ascending=True)

# We get the first from a ordered rank on #connections
least_popular = connections.first()

least_popular_conn = connections.filter(F.col('connections') == least_popular.connections)

least_popular_conn.show()

joined = least_popular_conn.join(hero_names, 'id')

joined.show()
# +----+-----------+--------------------+
# |  id|connections|                name|
# +----+-----------+--------------------+
# | 467|          1|        BERSERKER II|
# | 577|          1|              BLARE/|
# |3490|          1|MARVEL BOY II/MARTIN|
# |3489|          1|MARVEL BOY/MARTIN BU|
# |2139|          1|      GIURESCU, RADU|
# |1089|          1|       CLUMSY FOULUP|
# |1841|          1|              FENRIS|
# |4517|          1|              RANDAK|
# |5028|          1|           SHARKSKIN|
# | 835|          1|     CALLAHAN, DANNY|
# |1408|          1|         DEATHCHARGE|
# |4784|          1|                RUNE|
# |4945|          1|         SEA LEOPARD|
# |4602|          1|         RED WOLF II|
# |6411|          1|              ZANTOR|
# |3014|          1|JOHNSON, LYNDON BAIN|
# |3298|          1|          LUNATIK II|
# |2911|          1|                KULL|
# |2117|          1|GERVASE, LADY ALYSSA|
# +----+-----------+--------------------+