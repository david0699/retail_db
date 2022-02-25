package DataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object Customers {

  def getCustomers()(implicit sparksession:SparkSession): DataFrame = {
    sparksession
      .read
      .option("header", "true")
      .csv("src/main/resources/retail_db/customers/")
  }

  def getCustomersTabDelimited()(implicit sparksession:SparkSession): DataFrame = {
    sparksession
      .read
      .option("header", "false")
      .option("sep","\t")
      .csv("src/main/resources/retail_db/customers-tab-delimited/")
  }


}
