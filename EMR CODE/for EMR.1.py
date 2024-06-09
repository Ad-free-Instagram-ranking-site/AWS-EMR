from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("S3Merge").getOrCreate()
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.access.key", "")
hadoop_conf.set("fs.s3a.secret.key", "")
df = spark.read.json("s3a://seoultechs3/")

df.coalesce(1).write.json("s3a://seoultechs3./")

