import sys
from app.io.spark import spark_build
from app.config.helper import read_config
from pyspark.sql.types import *

spark = spark_build("data-write")
config = read_config()

cleansedpath = config['CleansedLayerSettings']['cleansedpath'] + "/" + sys.argv[1]

df = spark.read.option("header","true").parquet(cleansedpath)
df.show()
df.write.format('jdbc').options(
      url='jdbc:mysql://schipol-api.cq0xiuzh9tks.us-east-1.rds.amazonaws.com:3306/schipol_api',
      driver='com.mysql.jdbc.Driver',
      dbtable='flights',
      user='admin',
      password='Killzone2').mode('append').save()