from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark = SparkSession.builder.appName("S3Filter").getOrCreate()
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.access.key", "")
hadoop_conf.set("fs.s3a.secret.key", "")

df1 = spark.read.json("s3://seoultechs3/")
df2 = spark.read.json("s3a://seoultechs3/shopname.json")

shopnames = set(df2.select("shopname").rdd.flatMap(lambda x: x).collect())

filtered_data = []

df1_list = df1.rdd.map(lambda row: row.asDict()).collect()

for entry in df1_list:
    value_dict = json.loads(entry["맛집"])

    # Check both content and hashtags
    for field in ['content', 'tag']:
        if field in value_dict:
            # Check if any shopname is found in the field
            for shopname in shopnames:
                if shopname in value_dict[field]:
                    entry['shopname'] = shopname
                    filtered_data.append(entry)
                    break

filtered_df = spark.createDataFrame(filtered_data)

filtered_df.write.json("s3a://seoultechs3.2")