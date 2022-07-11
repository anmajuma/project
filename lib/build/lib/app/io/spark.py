from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def spark_build(app_name):
  builder = (
  SparkSession.builder
    .appName(app_name)
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config("spark.jars", "jars/mysql-connector-java-8.0.29.jar")
)
  spark = configure_spark_with_delta_pip(builder).getOrCreate()
  print("Session Created")
  return spark