package Exercices

import DataFrames.Orders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,from_unixtime, month, to_date, year}
import org.apache.spark.sql.types.{IntegerType, StringType}

object Exercise5 {
 def doExercise5()(implicit sparkSession: SparkSession): Unit ={
   val orders = Orders.getOrdersParquet()
     .select(col("order_id").cast(IntegerType)
       ,to_date(from_unixtime(col("order_date")/1000,"yyyy-mm-dd hh:mm:ss"),"yyyy-mm-dd hh:mm:ss").as("order_date")
     ,col("order_status"))
     .filter(col("order_status").equalTo("COMPLETE")
       && (month(col("order_date")).equalTo(7)
       || month(col("order_date")).equalTo(1))
       && year(col("order_date")).equalTo(2014))
     .orderBy(col("order_id"),col("order_date"))

   orders.coalesce(1)
     .write
     .mode("overwrite")
     .json("src/main/resources/exercises/q5")

   orders.printSchema()
   orders.show()
 }
}
