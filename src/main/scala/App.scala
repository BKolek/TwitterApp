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
    val clean: Dataset[Row] = cleaner.cleanTweets(loader.loadAllTweets)
    clean.show(truncate = false)
    clean.printSchema()
  }
}
