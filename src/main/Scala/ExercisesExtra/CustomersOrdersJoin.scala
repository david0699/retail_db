package ExercisesExtra

import org.apache.spark.sql.SparkSession
import DataFrames.{Customers, Orders}
import org.apache.spark.sql.functions.{broadcast, col, concat_ws}
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

    val customerOrders = orders.join(broadcast(customers),orders("_c2") <=> customers("customer_id"),"left")
      .groupBy("customer_id")
      .count()

    customerOrders.printSchema()

    customers.join(broadcast(customerOrders),customers("customer_id") <=> customerOrders("customer_id"),"inner")
      .select(customers("customer_id")
        ,col("customer_name")
        ,col("count").as("orders_count"))
      .dropDuplicates("customer_id")
      .orderBy(col("customer_id"))
      .show()
  }
}
