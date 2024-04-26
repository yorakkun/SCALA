import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import breeze.plot._

object Visualisation {

  def plotHistogram(spark: SparkSession, df: DataFrame, column: String, title: String): Figure = {
    import spark.implicits._
    val data = df.select(column).as[Double].collect()
    
    val f = Figure()
    val p = f.subplot(0)
    p += hist(data, 10) // Change the number of bins as needed
    p.xlabel = s"$column value"
    p.ylabel = "Frequency"
    p.title = title
    f
  }
  
  // Add more visualization functions as needed.
}
