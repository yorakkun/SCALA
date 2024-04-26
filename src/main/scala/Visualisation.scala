import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import breeze.plot._

object Visualisation {

  def plotHistogram(spark: SparkSession, df: DataFrame, column: String, title: String): Figure = {
    import spark.implicits._
    val data = df.select(column).as[Double].collect()

    val f = Figure()
    val p = f.subplot(0)
    p += hist(data, 10)
    p.xlabel = s"$column value"
    p.ylabel = "Frequency"
    p.title = title
    f
  }

  def plotBarChart(spark: SparkSession, df: DataFrame, xColumn: String, yColumn: String, title: String): Figure = {
    import spark.implicits._
    val data = df.select(xColumn, yColumn).as[(Double, Int)].collect()

    val f = Figure()
    val p = f.subplot(0)
    p += plot(data.map(_._1), data.map(_._2.toDouble), style = '.')
    p.xlabel = xColumn
    p.ylabel = yColumn
    p.title = title
    f
  }

  def plotScatterChart(spark: SparkSession, df: DataFrame, xColumn: String, yColumn: String, title: String): Figure = {
    import spark.implicits._
    val data = df.select(xColumn, yColumn).as[(Double, Double)].collect()

    val f = Figure()
    val p = f.subplot(0)
    p += plot(data.map(_._1), data.map(_._2), '.', colorcode = "red")
    p.xlabel = xColumn
    p.ylabel = yColumn
    p.title = title
    f
  }
}
