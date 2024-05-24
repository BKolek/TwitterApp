package Loaders

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}
object Load{
  val COVID_LABEL: String = "covid"
  val GRAMMY_LABEL: String = "grammy"
  val FINANCE_LABEL: String = "finance"
}
class Load(spark: SparkSession) {
  def loadFin: Dataset[Row]={
    spark.read
      .option("header", "true")
      .csv(path = "finance.csv")
      .withColumn(colName = "category", lit(Load.FINANCE_LABEL))
      .na.drop()
  }
  def loadGrammy: Dataset[Row] ={
    spark.read
      .option("header", "true")
      .csv(path="grammy.csv")
      .withColumn(colName = "category", lit(Load.GRAMMY_LABEL))
      .na.drop()

  }
  def loadCovid: Dataset[Row] ={
    spark.read
      .option("header", "true")
      .csv(path = "covid.csv")
      .withColumn(colName = "category", lit(Load.COVID_LABEL))
      .na.drop()
  }
  def loadAllTweets: Dataset[Row] ={
    val covid: Dataset[Row] = loadCovid
    val grammy: Dataset[Row] = loadGrammy
    val finance: Dataset[Row] = loadFin


    covid.unionByName(finance, allowMissingColumns = true)
      .unionByName(grammy, allowMissingColumns = true)
  }
}
