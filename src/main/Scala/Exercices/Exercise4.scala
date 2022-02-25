package Exercices

import DataFrames.Categories
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.DataTypes

object Exercise4 {
  def doExercise4()(implicit sparkSession: SparkSession): Unit ={
    val categories = Categories.getCategories()
      .withColumnRenamed("_c0","category_id")
      .withColumnRenamed("_c1","category_depart_id")
      .withColumnRenamed("_c2","category_name")
      .withColumn("category_id",col("category_id").cast(DataTypes.IntegerType))
      .withColumn("category_depart_id",col("category_depart_id").cast(DataTypes.IntegerType))

    val filterCategories = categories
      .filter("category_name like 'Soccer'")
      .select(concat_ws("|",col("category_id"),col("category_depart_id"),col("category_name")).cast(DataTypes.StringType).as("Value"))

    filterCategories.write
      .text("src/main/resources/exercises/q4")


  }
}
