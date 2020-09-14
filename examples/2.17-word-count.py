"""

"""
import re
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


spark = SparkSession.builder.appName('WoldCounter').getOrCreate()

schema = StructType([
    StructField('line', StringType(), True)
])

df = spark.read.csv('datasets/Book', sep='\n', schema=schema)

# Use regular expression to split line and use just lower Words
def counter_words(line:str) -> list:
    return re.compile(r'\W+', re.UNICODE).split(line.lower())


udf_spliter = F.udf(counter_words, ArrayType(StringType()))

df = df.withColumn('words', udf_spliter('line'))
print(df)
df.show(5)

df = df.withColumn('word', F.explode(df.words)) \
    .groupBy('word') \
    .count() \
    .sort('count', ascending=False)

df.show(5)
# +----+-----+
# |word|count|
# +----+-----+
# | you| 1878|
# |  to| 1828|
# |your| 1420|
# | the| 1292|
# |   a| 1191|
# +----+-----+

total_words = df.groupby().sum()
total_words.show(5)
# +----------+
# |sum(count)|
# +----------+
# |     48346|
# +----------+


## USING RDD
# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf = conf)
#
# input = sc.textFile("file:///sparkcourse/book.txt")
# words = input.flatMap(lambda x: x.split())
# wordCounts = words.countByValue()
#
# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii', 'ignore')
#     if (cleanWord):
#         print(cleanWord.decode() + " " + str(count))