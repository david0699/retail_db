package Exercices

import org.apache.spark.sql.SparkSession
import DataFrames.Customers
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DataTypes}

object Exercise3 {
  def doExercise3()(implicit sparksession:SparkSession): Unit ={
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

    customers.printSchema()
  }
}
