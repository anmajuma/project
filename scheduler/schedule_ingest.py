import subprocess

spark_submit_str= "spark-submit --master spark://spark-VirtualBox:7077 --executor-memory 10G --total-executor-cores 2 --py-files /home/spark/project/lib/dist/demolib-1.0-py2-none-any.whl /home/spark/project/job/ingest-api.py 'flights' "
process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)
stdout,stderr = process.communicate()
if process.returncode !=0:
   print(stderr)
print(stdout)