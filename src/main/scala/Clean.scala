import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Clean(spark: SparkSession) {
  // Define the UDF to remove the first and last characters
  val removeFirstLastUDF = udf((s: String) => s.slice(1, s.length - 1))

  // Method to clean tweets DataFrame
  def cleanTweets(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("date", col("date").cast(DateType))
      .withColumn("user_created", col("user_created").cast(DateType))
      .withColumn("user_favourites", col("user_favourites").cast(LongType))
      .withColumn("user_friends", col("user_friends").cast(LongType))
      .withColumn("user_followers", col("user_followers").cast(LongType))
      .withColumn("user_verified", col("user_verified").cast(BooleanType))
      .withColumn("hashtags", removeFirstLastUDF(col("hashtags")))
      .withColumn("hashtags", split(col("hashtags"), ", "))
      .withColumn("hashtags", expr("transform(hashtags, x -> substring(x, 2, length(x) - 2))"))

  }
}

