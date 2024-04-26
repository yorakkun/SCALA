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
  // def plotBarChartForTopics(dataFrame: DataFrame, spark: SparkSession): Unit = {
  // import spark.implicits._

  // val topicsData = dataFrame.select($"topic", explode($"termIndices").alias("term"), explode($"termWeights").alias("weight"))
  // val words = Array("word1", "word2", "word3", "word4", "word5") // Assume we have a mapping of indices to words

  // val plotData = topicsData.map { case Row(topic: Int, term: Int, weight: Double) =>
  //   (topic, words(term), weight)
  // }.toDF("topic", "word", "weight")

  // Vegas("Topic Modeling Visualization")
  //   .withDataFrame(plotData)
  //   .mark(Bar)
  //   .encodeX("word", Ordinal, scale=Scale(bandSize=20))
  //   .encodeY("weight", Quantitative)
  //   .encodeColor(field="topic", dataType=Nominal)
  //   .show
  // }
}
