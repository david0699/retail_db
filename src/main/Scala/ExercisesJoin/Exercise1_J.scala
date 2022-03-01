package ExercisesJoin

import DataFrames.{Customers, Orders}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, desc}

object Exercise1_J {
  def doExercise1_J()(implicit sparkSession: SparkSession):Unit={
    val customers = Customers.getCustomers()
      .select(col("customer_id"),col("customer_fname"),col("customer_lname"))
    val orders = Orders.getOrders()

    val customersOrdersCount = orders.join(broadcast(customers),orders("_c2")<=>customers("customer_id"),"inner")
      .groupBy("customer_id")
      .count()

    val finalCustomerOrders = customers.join(broadcast(customersOrdersCount),customers("customer_id")<=>customersOrdersCount("customer_id"),"inner")
      .select(customers("customer_id")
        ,col("customer_fname")
        ,col("customer_lname")
        ,col("count").as("orders"))
      .filter(col("orders")>5
        && col("customer_fname").startsWith("M"))
      .dropDuplicates()
      .sort(desc("orders"))

    finalCustomerOrders.coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .option("sep","|")
      .option("compression","gzip")
      .csv("src/main/resources/exercisesJoin/q1/")
  }
}
