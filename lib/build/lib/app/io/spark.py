from pyspark.sql import SparkSession

def spark_build(app_name):
  spark = SparkSession.builder.appName(app_name).getOrCreate()
  print("Session Created")
  return spark