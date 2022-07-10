import sys
from app.io.spark import spark_build
from app.config.helper import read_config
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField, StringType,TimestampType
from pyspark.sql.functions import *

# total arguments
n = len(sys.argv)
print("Total arguments passed:", n)
 
# Arguments passed
print("\nName of Python script:", sys.argv[0])
#print("\nName of filepath:", sys.argv[1])


spark = spark_build("data-cleanse")
config = read_config()
landingpath = config['LandingLayerSettings']['landingpath'] + "/" + sys.argv[1]
archivepath = config['ArchiveLayerSettings']['archivepath'] + "/" + sys.argv[1]
cleansedpath = config['CleansedLayerSettings']['cleansedpath'] + "/" + sys.argv[1]

# Read from HDFS
df_load = spark.read.option("header","true").parquet(landingpath)
df_load.show()

try:
   df_old = spark.read.option("header","true").parquet(archivepath)
   
   df_new =df_load.subtract(df_old)
   df_new.show()
   if df_new.count() != 0 :
    print("Updates came")

   else :
        print("No change in records")
        
except:
    print("Initial Load")
    df_new = df_load
    
 
schema = StructType([ 
    StructField("scheduleDate",StringType(),True), 
    StructField("scheduleTime",StringType(),True), 
    StructField("actualLandingTime", TimestampType(), True) ,
    StructField("lastUpdatedAt", TimestampType(), True)
  ])

dfJSON = df_new.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("flnm","jsonData.*")
dfJSON.printSchema()
dfJSON.show()
try:
        if dfJSON.count() != 0 :
            df_new.write.mode("append").option("header","true").parquet(archivepath)
            print("Archived Records Written")
            dfJSON.write.mode("overwrite").option("header","true").parquet(cleansedpath)
            print("Data Cleansing Successed")
  
except :
    print("Data Cleansing Failed")