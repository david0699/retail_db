package DataFrames

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Categories {
  def getCategoriesWithHeader()(implicit sparkSession: SparkSession):DataFrame={

    sparkSession
      .read
      .option("header","true")
      .csv("src/main/resources/retail_db/categories-header/categories-header_part-00000.csv")
  }

  def getCategories()(implicit sparkSession: SparkSession):DataFrame={

    sparkSession
      .read
      .option("header","false")
      .csv("src/main/resources/retail_db/categories/part-m-00000")
  }
}
