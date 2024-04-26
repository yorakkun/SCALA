import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import Nettoyage.ClearData
import Visualisation.plotHistogram
import Visualisation.plotBarChart
import Visualisation.plotScatterChart

object SimpleApp extends App {
  val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
  import spark.implicits._

  // Obtain the cleaned DataFrame directly from the ClearData function
  val cleanedData = ClearData(spark)

  // Identify lines containing "ISBN" as new book starts and assign IDs
  val withBookStarts = cleanedData.withColumn("isNewBook", lower($"clean_value").contains("isbn"))
  val bookIds = withBookStarts.withColumn("bookId", sum(when($"isNewBook", 1).otherwise(0)).over(Window.orderBy(monotonically_increasing_id())))

  // Filter out the ISBN lines if you want only content, adjust as needed
  val contentData = bookIds.filter(!$"isNewBook")

  // Group by 'bookId' and aggregate contents
  val books = contentData.groupBy($"bookId").agg(collect_list($"clean_value").as("content"))

  // DF pour conter les lignes par livres (elles seront supprimées dans l'étape suivante)
  val analysisArray = contentData.groupBy($"bookId").agg(count("*").as("lines"))

  // Convert the aggregated content into a single string per book and assign IDs
  val finalBooks = books.withColumn("id", monotonically_increasing_id())
                        .withColumn("content", concat_ws(" ", $"content"))
                        .select("id", "content") // Select only the necessary columns

  // Convert to Dataset of a case class if needed for better type safety
  finalBooks.as[(Long, String)].show(true)

  // Lignes pour le calcul de la moyenne et mediane TOTALE
  val nbLignes = cleanedData.count()

  // Calcul des mots PAR LIVRE
  val finalBooksMots = finalBooks.withColumn("mots", size(split(col("content"), "\\s+")))

  val joinedDF = finalBooksMots.join(analysisArray, finalBooksMots("id") === analysisArray("bookId"), "left")

  val finalAnalysis = joinedDF.withColumn("Mots/Ligne", $"mots"/$"lines")
                      .select("id", "mots", "lines", "Mots/Ligne")

  val motsLigne = finalBooks.selectExpr("split(content, ' ') as mots")

  val allMots = motsLigne.selectExpr("explode(mots) as mot")

  val nbMots = allMots.count()

  // Calcul de la moyenne de mots par ligne
  val moyMots = nbMots/nbLignes

  // Calcul de la mediane de mots par ligne
  val aName = allMots.selectExpr("length(mot) as lg")
  val lignes = aName.orderBy("lg").select("lg").collect()
  val medMots = lignes((nbMots/2).toInt)

  println(s"Il y a $nbLignes Lignes dans le texte")
  println(s"Il y a $nbMots Mots dans le texte")
  println(s"Il y a $nbLignes Lignes dans le texte")
  println(s"Il y a en moyenne $moyMots Mots par Ligne dans le texte")
  println(s"Il y a en mediane $medMots Mots par Ligne dans le texte")

  // Histogramme des mots par ligne
  val histogram = plotHistogram(spark, finalAnalysis, "Mots/Ligne", "Histogramme de Mots par Ligne")
  histogram.saveas("histogram_mots_par_ligne.png")

  // Graphique à barres du nombre de mots par livre
  val barChart = plotBarChart(spark, finalAnalysis, "id", "mots", "Bar Chart de Mots par Livre")
  barChart.saveas("bar_chart_mots_par_livre.png")

  // Graphique de dispersion du nombre de mots contre lignes
  val scatterChart = plotScatterChart(spark, finalAnalysis, "mots", "lines", "Scatter Chart Mots contre Lignes")
  scatterChart.saveas("scatter_chart_mots_contre_lignes.png")
  
  spark.stop()
}
