import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Search(spark: SparkSession) {
  def searchByKeyWord(kw: String)(df: Dataset[Row]) = {
    df.filter(col("text").contains(kw))
  }

  def searchByKeywords(keywords: Seq[String])(df: Dataset[Row]): Dataset[Row] = {
    val keywordConditions = keywords.map(kw => col("text").contains(kw))
    df.filter(keywordConditions.reduce(_ or _))
  }
}
