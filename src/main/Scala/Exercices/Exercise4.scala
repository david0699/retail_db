package Exercices

import DataFrames.Categories
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.DataTypes._

object Exercise4 {
  def doExercise4()(implicit sparkSession: SparkSession): Unit = {
    val filterCategories = Categories.getCategories()
      .select(
        col("_c0").cast(IntegerType),col("_c1").cast(IntegerType),col("_c2"))
      .filter(col("_c2").equalTo("Soccer"))

    filterCategories.write
      .mode("overwrite")
      .option("sep","|")
      .csv("src/main/resources/exercises/q4")


  }
}
