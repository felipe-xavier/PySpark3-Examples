"""

"""

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder.appName('CustomerAmount').getOrCreate()

schema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('order_id', IntegerType(), True),
    StructField('value', FloatType(), True)
])

df = spark.read.csv('datasets/customer-orders.csv', schema=schema)
df.describe().show()
df.show()

df_users = df.groupby('user_id')\
    .agg(F.sum('value').alias('total'))\
    .sort('total', ascending=False)

df_users.show()