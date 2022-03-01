
import org.apache.spark.sql.SparkSession
import Exercices.{Exercise1, Exercise2, Exercise3, Exercise4, Exercise5, Exercise6}
import ExercisesExtra.{CustomersOrdersJoin, ProductsCategoriesEditProducts, ProductsCategoriesJoin}


object Main extends App {
  implicit val sparkSession: SparkSession = Spark.getSparkSession


  sparkSession.sparkContext.setLogLevel("ERROR")

  //Exercise1.doExercise1()

  //Exercise2.doExercise2()

  //Exercise3.doExercise3()

  //Exercise4.doExercise4()

  //Exercise5.doExercise5()

  //Exercise6.doExercise6()

  //CustomersOrdersJoin.countOrdersPerCustomer()

  //ProductsCategoriesJoin.getCategoryForProduct()

  ProductsCategoriesEditProducts.getEditProducts()

}
