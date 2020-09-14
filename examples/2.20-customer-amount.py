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
df.show(5)
# +-------+--------+-----+
# |user_id|order_id|value|
# +-------+--------+-----+
# |     44|    8602|37.19|
# |     35|    5368|65.89|
# |      2|    3391|40.64|
# |     47|    6694|14.98|
# |     29|     680|13.08|
# +-------+--------+-----+

df_users = df.groupby('user_id')\
    .agg(F.sum('value').alias('total'))\
    .sort('total', ascending=False)

df_users.show(5)
# +-------+-----------------+
# |user_id|            total|
# +-------+-----------------+
# |     68|6375.450028181076|
# |     73|6206.199985742569|
# |     39|6193.109993815422|
# |     54|6065.390002984554|
# |     71|5995.659991919994|
# +-------+-----------------+