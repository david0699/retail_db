package Exercices

import org.apache.spark.sql.SparkSession
import DataFrames.Customers
import org.apache.spark.sql.functions.{col, concat, concat_ws, count}
import org.apache.spark.sql.types.{DataType, DataTypes, StringType}

object Exercise3 {
  def doExercise3()(implicit sparkSession:SparkSession): Unit ={
    val customers = Customers.getCustomersTabDelimited()
      .withColumnRenamed("_c0","customer_id")
      .withColumnRenamed("_c1","customer_fname")
      .withColumnRenamed("_c2","customer_lname")
      .withColumnRenamed("_c3","customer_email")
      .withColumnRenamed("_c4","customer_password")
      .withColumnRenamed("_c5","customer_street")
      .withColumnRenamed("_c6","customer_city")
      .withColumnRenamed("_c7","customer_state")
      .withColumnRenamed("_c8","customer_zipcode")
      .withColumn("customer_id",col("customer_id").cast(DataTypes.IntegerType))

    val countCustomersState = customers
      .filter(col("customer_fname").like("a%").like("A%"))
      .groupBy("customer_state")
      .count()
      .filter(col("count") > 50)
      .select(concat_ws(",",col("customer_state"),col("count")).cast(StringType).as("Value"))


    countCustomersState.write
      .mode("overwrite")
      .option("header","true")
      .format("parquet")
      .option("compression","gzip")
      .save("src/main/resources/exercises/q3")

  }
}
