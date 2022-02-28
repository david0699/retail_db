package ExercisesExtra

import org.apache.spark.sql.SparkSession
import DataFrames.{Customers, Orders}
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.IntegerType

object CustomersOrdersJoin {
  def countOrdersPerCustomer()(implicit sparkSession: SparkSession):Unit={
    val customers = Customers.getCustomers()
      .select(col("customer_id").cast(IntegerType)
        ,concat_ws(" "
          ,col("customer_fname")
          ,col("customer_lname"))
          .as("customer_name"))

    val orders = Orders.getOrders()
    customers.printSchema()
    customers.show()
    orders.printSchema()
    orders.show()

    customers.join(orders,customers("customer_id") === orders("_c2"),"left")
      .select(col("customer_id"),col("customer_name"))
      .groupBy("customer_id")
      .count()
      .orderBy(col("customer_id"))
      .show()
  }
}
