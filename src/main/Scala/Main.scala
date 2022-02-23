import org.apache.spark.sql.SparkSession

object Main extends App {
  val sparksession = Spark.getSparkSession


  sparksession.sparkContext.setLogLevel("ERROR")
  val customers = sparksession
    .read
    .option("header","true")
    .csv("src/main/resources/retail_db/customers/part-m-00000 - Copy")

  customers.show()
}
