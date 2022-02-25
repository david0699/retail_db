import DataFrames.{Categories, Customers, Products}
import org.apache.spark.sql.SparkSession
import Exercices.{Exercise2,Exercise3}

object Main extends App {
  implicit val sparksession: SparkSession = Spark.getSparkSession


  sparksession.sparkContext.setLogLevel("ERROR")

  /*
  val customers = Customers.getCustomersTabDelimited()
  customers.show()
  */

  /*
  val products = Products.getProductsAvro()
  products.show()
  */


  Exercise2.doExercise2()

  Exercise3.doExercise3()



}
