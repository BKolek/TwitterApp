import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



class Clean(spark: SparkSession) {
  import ColumnNames._

  val removeFirstLastUDF = udf((s: String) => s.slice(1, s.length - 1))


  def cleanTweets(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn(DATE, col(DATE).cast(DateType))
      .withColumn(USER_CREATED, col(USER_CREATED).cast(DateType))
      .withColumn(USER_FAVOURITES, col(USER_FAVOURITES).cast(LongType))
      .withColumn(USER_FRIENDS, col(USER_FRIENDS).cast(LongType))
      .withColumn(USER_FOLLOWERS, col(USER_FOLLOWERS).cast(LongType))
      .withColumn(USER_VERIFIED, col(USER_VERIFIED).cast(BooleanType))
      .withColumn(HASHTAGS, removeFirstLastUDF(col(HASHTAGS)))
      .withColumn(HASHTAGS, split(col(HASHTAGS), ", "))
      .withColumn(HASHTAGS, expr("transform(hashtags, x -> substring(x, 2, length(x) - 2))"))
  }
}