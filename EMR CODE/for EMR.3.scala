import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .appName("Your Application Name")
  .config("spark.hadoop.fs.s3a.access.key", "")
  .config("spark.hadoop.fs.s3a.secret.key", "")
  .getOrCreate()

val df = spark.read.json("s3://seoultechs3/filtered_data2.json/*.json")
val schema = new StructType()
  .add("writer", StringType)
  .add("content", StringType)
  .add("date", StringType)
  .add("like", StringType)
  .add("place", StringType)
  .add("tag", ArrayType(StringType))

val df_with_details = df.withColumn("맛집", from_json($"맛집", schema))

val df_flat = df_with_details.select("shopname", "맛집.*")

val df_renamed = df_flat.withColumnRenamed("shopname", "shop_name")
  .withColumnRenamed("like", "likes")

val dfWithIntLikes = df_renamed.withColumn("likes", col("likes").cast(IntegerType))

val dfGrouped = dfWithIntLikes.groupBy("shop_name")
    .agg(count("*").as("total_posts"), sum("likes").as("total_likes"))

val totalPosts = dfWithIntLikes.count()
val totalLikes = dfWithIntLikes.agg(sum("likes")).first().getLong(0)

val avgLikesPerPost = totalLikes.toDouble / totalPosts

val dfWithScore = dfGrouped.withColumn("score", col("total_posts") * avgLikesPerPost + col("total_likes"))

val dfSorted = dfWithScore.orderBy(col("score").desc)
dfSorted.show()
dfSorted.write
  .format("json")
  .mode("overwrite") 
  .option("path", "s3://seoultechs3.3/mydata")
  .save()