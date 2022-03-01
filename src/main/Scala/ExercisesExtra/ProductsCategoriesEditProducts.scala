package ExercisesExtra

import DataFrames.{Categories, Products}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, row_number}

object ProductsCategoriesEditProducts {
  def getEditProducts()(implicit sparkSession: SparkSession):Unit={
    val categories = Categories.getCategoriesWithHeader()
    categories.printSchema()
    val products = Products.getProductsAvro()
    products.printSchema()

    val productsCategoriesJoin = products.join(categories,products("product_category_id") === categories("category_id"),"left")
      .select(col("product_id")
        ,col("product_category_id")
        ,col("product_name")
        ,col("product_description")
        ,col("product_price")
        ,col("product_image")
        ,col("category_name"))
      .where(col("product_price").between(50,80)
        && col("category_name").equalTo("World Cup Shop"))
      .withColumn("product_price",col("product_price")*0.8)

    productsCategoriesJoin.show()

    val listToDrop = productsCategoriesJoin.select(col("product_id")).rdd.map(r=>r.getInt(0)).collect().toList
    /*
    dropDf.write
      .mode("overwrite")
      .option("header","true")
      .csv("src/main/resources/exercisesExtra/ProductsCategoriesExerciseEditProduct/")

     */


  }
}
