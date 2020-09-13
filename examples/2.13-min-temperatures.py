from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType


spark = SparkSession.builder.appName('MinTemp').getOrCreate()

# We will set just the necessary first four fields for our example.
schema = StructType([
    StructField("stn_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("var", StringType(), True),
    StructField("value", FloatType(), True)
])

pcds_df = spark.read.csv('examples/datasets/1800.csv', schema=schema)

pcds_df.describe().show()
pcds_df.printSchema()

# Select just the rows with TMIN in var column.
min_temps_df = pcds_df[pcds_df.var == 'TMIN']
min_temps_df.show(5)

# Check how many stations we have using distinct().
min_temps_df.select('stn_id').distinct().show()
# +-----------+
# |     stn_id|
# +-----------+
# |ITE00100554|
# |EZE00100082|
# +-----------+

min_temps_stns_df = min_temps_df\
    .groupby('stn_id')\
    .agg(F.min('value'))\
    .orderBy('min(value)')\
    .cache()

def calc_F(value):
    return value * 0.1 * (9.0/5.0) + 32

udf_calc_F = F.udf(calc_F, FloatType())

min_temps_stns_df = min_temps_stns_df.withColumn('value_F',
                                                 udf_calc_F('min(value)'))

min_temps_stns_df.show()

# +-----------+----------+-------+
# |     stn_id|min(value)|value_F|
# +-----------+----------+-------+
# |ITE00100554|    -148.0|   5.36|
# |EZE00100082|    -135.0|    7.7|
# +-----------+----------+-------+


## USING RDD from the Course:

# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
# sc = SparkContext(conf = conf)
#
# def parseLine(line):
#     fields = line.split(',')
#     stationID = fields[0]
#     entryType = fields[2]
#     temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
#     return (stationID, entryType, temperature)
#
# lines = sc.textFile("file:///SparkCourse/1800.csv")
# parsedLines = lines.map(parseLine)
# minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# results = minTemps.collect()
#
# for result in results:
#     print(result[0] + "\t{:.2f}F".format(result[1]))
