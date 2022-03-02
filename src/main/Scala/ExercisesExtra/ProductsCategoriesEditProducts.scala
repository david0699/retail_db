package ExercisesExtra

import DataFrames.{Categories, Products}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when,round}



object ProductsCategoriesEditProducts {
  def getEditProducts()(implicit sparkSession: SparkSession):Unit={
    val categories = Categories.getCategoriesWithHeader()
    categories.printSchema()
    val products = Products.getProductsAvro()
    products.printSchema()

    val productsCategoriesJoin = products.join(categories,products("product_category_id") <=> categories("category_id"),"left")
      .select(col("product_id")
        ,col("product_category_id")
        ,col("product_name")
        ,col("product_description")
        ,col("product_price")
        ,col("product_image")
        ,col("category_name"))
      .withColumn("product_price",
        when(col("product_price").between(50,80)
        && col("category_name").equalTo("World Cup Shop")
        ,col("product_price")*0.8)
      .otherwise(round(col("product_price")*100)/100))
      .drop(col("category_name"))

    productsCategoriesJoin.coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .csv("src/main/resources/exercisesExtra/ProductsCategoriesExerciseEditProduct/")



  }
}
