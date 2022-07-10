import sys
from app.io.spark import spark_build
from app.config.helper import read_config
from pyspark.sql.types import *

# total arguments
n = len(sys.argv)
print("Total arguments passed:", n)
 
# Arguments passed
print("\nName of Python script:", sys.argv[0])
print("\nName of filepath:", sys.argv[1])


spark = spark_build("data-cleanse")
config = read_config()
landingpath = config['LandingLayerSettings']['landingpath'] + "/" + sys.argv[1]
# Read from HDFS
df_load = spark.read.option("header","true").parquet(landingpath)
df_load.show()