package DataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object Customers {

  def readCustomersCsv()(implicit sparksession:SparkSession): DataFrame = {
    sparksession
      .read
      .option("header", "true")
      .csv("src/main/resources/retail_db/customers/part-m-00000 - Copy")
  }


}
