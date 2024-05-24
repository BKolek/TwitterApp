import org.apache.spark.sql.functions.{avg, col, desc, explode, explode_outer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Analyzer(spark: SparkSession) {
  import ColumnNames._

  def calculateAvgFollowPerLocation(df: Dataset[Row]): Dataset[Row] = {

    df.select(USER_NAME, USER_FOLLOWERS, USER_LOCATION)
      .filter(col(USER_NAME).isNotNull)
      .filter(col(USER_LOCATION).isNotNull)
      .filter(col(USER_FOLLOWERS).isNotNull)
      .dropDuplicates(USER_NAME)
      .groupBy(USER_LOCATION)
      .agg(avg(USER_FOLLOWERS).as(AVERAGE_FOLLOWERS))
      .withColumnRenamed(AVERAGE_FOLLOWERS, COUNT)
  }
  def calculateByRetweets(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy(IS_RETWEET).count()
      .orderBy(desc(COUNT))
  }

}
