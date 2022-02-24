import Main.sparksession
import org.apache.spark.sql.{DataFrame, SparkSession}

object Products {
  def getProductsAvro()(implicit  sparksession:SparkSession):DataFrame={
    sparksession
      .read
      .format("avro")
      .option("header","true")
      .load("src/main/resources/retail_db/products_avro/part-m-00000.avro")
  }
}
