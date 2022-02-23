import org.apache.spark.sql.SparkSession

object Main extends App {
  implicit val sparksession = Spark.getSparkSession


  sparksession.sparkContext.setLogLevel("ERROR")
  val customers = Customers.readCustomersCsv()

  customers.show()
}
