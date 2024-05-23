import org.apache.spark.sql.functions.{avg, col, desc, explode, explode_outer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Analyzer(spark: SparkSession) {
  def calculateTag(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("hashtag", explode_outer(col("hashtags")))
      .groupBy("hashtags").count()
  }
  def calculateAvgFollowPerLocation(df: Dataset[Row]) = {
    df.select("user_name", "user_followers", "user_location")
      .filter(col("user_name").isNotNull)
      .filter(col("user_location").isNotNull)
      .filter(col("user_followers").isNotNull)
      .dropDuplicates("user_name")
      .groupBy("user_location")
      .count()
      .withColumnRenamed("count(user_followers)", ("count"))

  }
  def calculateByRetweets(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy("is_retweet").count()
      .orderBy(desc("count"))
  }

}
