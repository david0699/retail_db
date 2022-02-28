package ExercisesExtra

import DataFrames.{Categories, Products}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ProductsCategoriesJoin {
def getCategoryForProduct()(implicit sparkSession: SparkSession):Unit={
  val categories = Categories.getCategoriesWithHeader()
  categories.printSchema()
  val products = Products.getProductsAvro()
  products.printSchema()

  val productsCategoriesJoin = products.join(categories,products("product_category_id") === categories("category_id"),"left")
    .select(col("product_id")
      ,col("product_name")
      ,(col("product_price"))
      ,col("category_id")
      ,col("category_name"))
    .where(col("product_price").between(50,80)
      && col("category_name").equalTo("World Cup Shop"))

  productsCategoriesJoin.write
    .mode("overwrite")
    .option("header","true")
    .csv("src/main/resources/exercisesExtra/ProductsCategoriesExercise/")

}
}
