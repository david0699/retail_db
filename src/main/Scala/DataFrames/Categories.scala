package DataFrames

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Categories {
  def getCategories()(implicit sparksession: SparkSession):DataFrame={
    case class category(categoryId: Integer, categoryDepartId: Integer, categoryName: String)

    import sparksession.implicits._
    sparksession
      .read
      .option("header","true")
      .csv("src/main/resources/retail_db/categories-header/categories-header_part-00000.csv")
  }
}
