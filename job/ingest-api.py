import json
from app.io.spark import spark_build
from app.io.api import call_api
from pyspark.sql.types import *
from app.config.helper import read_config

spark = spark_build("ingest-api")
config = read_config()
landingpath = config['LandingLayerSettings']['landingpath'] 
# Creating an empty RDD to make a DataFrame with no data
emp_RDD = spark.sparkContext.emptyRDD()
# Defining the schema of the DataFrame
schema = StructType([StructField('flnm', StringType(), False),
                       StructField('value', StringType(), False)])

# Creating an empty DataFrame
first_df = spark.createDataFrame(data=emp_RDD,schema=schema)

# Printing the DataFrame with no data
first_df.show()
# Getting FlightList from API Call
flightList = call_api()

for flight in flightList['flights']:

            # Convert dict to string
            data = json.dumps(flight)
            #print(data)
            flnm = flight['flightName']
            #print(flnm)
            df=spark.createDataFrame([(flnm,data)],["flnm","value"])
            first_df = first_df.union(df)
# Write into HDFS            
try:
            first_df.write.mode("overwrite").option("header","true").csv(landingpath)
            print("Data Ingestion Successed")    
except :
            print("Data Ingestion Failed") 
           