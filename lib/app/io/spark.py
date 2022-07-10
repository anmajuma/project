from pyspark.sql import SparkSession

def spark_build(app_name):
  spark = SparkSession.builder.config("spark.jars", "jars/mysql-connector-java-8.0.29.jar").appName(app_name).getOrCreate()
  print("Session Created")
  return spark