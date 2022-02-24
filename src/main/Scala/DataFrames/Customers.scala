package DataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object Customers {

  def getCustomers()(implicit sparksession:SparkSession): DataFrame = {
    sparksession
      .read
      .option("header", "true")
      .csv("src/main/resources/retail_db/customers/part-m-00000 - Copy")
  }

  def getCustomersTabDelimited()(implicit sparksession:SparkSession): DataFrame = {
    sparksession
      .read
      .option("header", "false")
      .option("sep","\t")
      .csv("src/main/resources/retail_db/customers-tab-delimited/part-m-00000")
  }


}
