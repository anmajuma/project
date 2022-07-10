from app.io.spark import spark_build
from app.config.helper import read_config
from pyspark.sql.types import *

spark = spark_build("data-cleanse")
config = read_config()
landingpath = config['LandingLayerSettings']['landingpath']
# Read from HDFS
df_load = spark.read.option("header","true").parquet(landingpath)
df_load.show()