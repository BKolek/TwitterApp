import org.apache.spark
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Load(spark: SparkSession) {
  def loadFin: Dataset[Row]={
    spark.read
      .option("header", "true")
      .csv(path = "finance.csv")
      .withColumn(colName = "category", lit("finance"))
      .na.drop()
  }
  def loadGrammy: Dataset[Row] ={
    spark.read
      .option("header", "true")
      .csv(path="grammy.csv")
      .withColumn(colName = "category", lit("grammy"))
      .na.drop()

  }
  def loadCovid: Dataset[Row] ={
    spark.read
      .option("header", "true")
      .csv(path = "covid.csv")
      .withColumn(colName = "category", lit("covid"))
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
