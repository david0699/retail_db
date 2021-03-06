package Exercices

import DataFrames.Categories
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DataTypes, IntegerType}

object Exercise2 {
  def doExercise2()(implicit sparkSession:SparkSession): Unit = {

    val categories = Categories.getCategoriesWithHeader()
      .withColumn("category_id", col("category_id").cast(DataTypes.IntegerType))

    val categories2 = categories
      .select(col("category_id").cast(IntegerType),col("category_name"))
      .sort(desc("category_id"))

    categories2.coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .option("sep",":")
      .csv("src/main/resources/exercises/q2")
  }
}
