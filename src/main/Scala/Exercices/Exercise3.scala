package Exercices

import org.apache.spark.sql.SparkSession
import DataFrames.Customers
import org.apache.spark.sql.functions.col

object Exercise3 {
  def doExercise3()(implicit sparkSession:SparkSession): Unit = {
    val countCustomersState = Customers.getCustomersTabDelimited()
      .filter(col("_c1").startsWith("A"))
      .groupBy("_c7")
      .count()
      .filter(col("count") > 50)

    countCustomersState.coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .option("compression","gzip")
      .parquet("src/main/resources/exercises/q3")

  }
}
