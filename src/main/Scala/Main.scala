import DataFrames.Customers
import org.apache.spark.sql.SparkSession

object Main extends App {
  implicit val sparksession: SparkSession = Spark.getSparkSession


  sparksession.sparkContext.setLogLevel("ERROR")

  val customers = Customers.readCustomersCsv()
  customers.show()

  /*
  val products = Products.getProductsAvro()
  products.show()
   */


}
