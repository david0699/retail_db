package DataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object Orders {
  def getOrdersParquet()(implicit sparkSession: SparkSession):DataFrame={
    sparkSession.read
      .option("header","true")
      .parquet("src/main/resources/retail_db/orders_parquet/")
  }

  def getOrders()(implicit sparkSession: SparkSession):DataFrame={
    sparkSession.read
      .option("header","false")
      .csv("src/main/resources/retail_db/orders/")
  }
}
