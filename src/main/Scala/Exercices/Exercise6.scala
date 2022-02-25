package Exercices

import DataFrames.Customers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws}

object Exercise6 {
  def doExercise6()(implicit sparkSession: SparkSession): Unit = {
    val customers = Customers.getCustomersAvro()
      .select(col("customer_id"),concat_ws(" ",col("customer_fname"),col("customer_lname")).as("customer_name"))

    customers.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression","bzip2")
      .option("sep","\t")
      .option("header","true")
      .csv("src/main/resources/exercises/q6")

    customers.show()
  }
}
