package Exercices

import DataFrames.Products
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Exercise1 {
  def doExercise1()(implicit sparkSession: SparkSession): Unit ={
    val filterProducts = Products.getProductsAvro()
      .filter(col("product_price").between(20,23) && col("product_name").startsWith("Nike"))

    filterProducts.write
      .mode("overwrite")
      .option("header","true")
      .option("compression","gzip")
      .parquet("src/main/resources/exercises/q1")

  }
}
