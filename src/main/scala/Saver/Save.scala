package Saver

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

class Save(spark: SparkSession) {
  def saveToDb(df: Dataset[Row]): Unit = {
    val jdbcUrl = "jdbc:postgresql://localhost:5433/postgres"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "mysecretpassword")
    connectionProperties.put("driver", "org.postgresql.Driver")

    // Save DataFrame to PostgreSQL
    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "mytable", connectionProperties)
  }

}
