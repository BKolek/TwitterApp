import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, IntegerType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object App {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(name = "income")
      .master(master = "local")
      .getOrCreate()

    val loader = new Load(spark)
    val cleaner = new Clean(spark)
    val cleanDF: Dataset[Row] = cleaner.cleanTweets(loader.loadAllTweets)
    val analyze = new Analyzer(spark)
    val searcher = new Search(spark)

    val words = searcher.searchByKeywords(Seq("Trump"))(cleanDF)
    val analyzed = analyze.calculateAvgFollowPerLocation(words)
    analyzed.orderBy(col("count").desc).show(truncate = false)
  }
}
