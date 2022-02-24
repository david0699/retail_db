import DataFrames.{Customers,Categories}
import org.apache.spark.sql.SparkSession
import Exercices.Exercise2

object Main extends App {
  implicit val sparksession: SparkSession = Spark.getSparkSession


  sparksession.sparkContext.setLogLevel("ERROR")
  /*
  val customers = Customers.readCustomersCsv()
  customers.show()
  */

  /*
  val products = Products.getProductsAvro()
  products.show()
   */

  val exercise2 = Exercise2.doExercise2()



}
