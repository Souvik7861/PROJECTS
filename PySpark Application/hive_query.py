
from pyspark.sql import SparkSession

warehouse_location = 'C:/Users/souvik/PycharmProjects/Pyspark_realtime_application/spark-warehouse'

spark = SparkSession.builder \
                 .master('local') \
                 .appName('hive_query') \
                 .enableHiveSupport() \
                 .config("spark.sql.warehouse.dir", warehouse_location) \
                 .getOrCreate()


df = spark.sql("select * from cities.df_city where state_name = 'COLORADO' ")
df.show()

