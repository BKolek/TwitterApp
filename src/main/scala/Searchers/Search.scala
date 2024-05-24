package Searchers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Search(spark: SparkSession) {
  import ColumnNames.ColumnNames._
  def searchByKeyWord(kw: String)(df: Dataset[Row]) = {


    df.filter(col(TEXT).contains(kw))
  }

  def searchByKeywords(keywords: Seq[String])(df: Dataset[Row]): Dataset[Row] = {
    val keywordConditions = keywords.map(kw => col(TEXT).contains(kw))
    df.filter(keywordConditions.reduce(_ or _))
  }
}
