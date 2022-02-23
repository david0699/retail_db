import org.apache.spark.sql.SparkSession
object Spark {
  def getSparkSession:SparkSession ={
    SparkSession
      .builder()
      .master("local[*]")
      .appName("retail_db")
      .config("spark.some.config.option","some-value")
      .getOrCreate()
  }
}
