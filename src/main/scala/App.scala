import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object App {
  import ColumnNames._
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(name = "income")
      .master(master = "local")
      .getOrCreate()

    val loader: Load = new Load(spark)
    val cleaner: Clean = new Clean(spark)
    val cleanDF: Dataset[Row] = cleaner.cleanTweets(loader.loadAllTweets)
    val analyze: Analyzer = new Analyzer(spark)
    val searcher: Search = new Search(spark)

    val words: Dataset[Row] = searcher.searchByKeywords(Seq("Trump"))(cleanDF)
    val analyzed: Dataset[Row] = analyze.calculateAvgFollowPerLocation(words)
    analyzed.orderBy(col(COUNT).desc).show(truncate = false)
  }
}
