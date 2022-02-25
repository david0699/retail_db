package DataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object Products {
  def getProductsAvro()(implicit sparksession: SparkSession): DataFrame = {
    sparksession.read
      .format("avro")
      .load("src/main/resources/retail_db/products_avro/")
  }
}
