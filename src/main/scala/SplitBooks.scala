import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import Nettoyage.ClearData

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
  val analysisArray = cleanedData.groupBy($"bookId").agg(count("*").as("lines"))

  // Convert the aggregated content into a single string per book and assign IDs
  val finalBooks = books.withColumn("id", monotonically_increasing_id())
                        .withColumn("content", concat_ws(" ", $"content"))
                        .select("id", "content") // Select only the necessary columns

  // Show results
  finalBooks.show(true)

  // Convert to Dataset of a case class if needed for better type safety
  finalBooks.as[(Long, String)].show(true)

  // Lignes pour le calcul de la moyenne et mediane TOTALE
  val nbLignes = cleanedData.count()

  // Calcul des mots PAR LIVRE
  val finalBooksMots = finalBooks.withColumn("mots", size(split(col("content"), "\\s+")))

  val joinedDF = finalBooksMots.join(analysisArray, finalBooksMots("id") === analysisArray("bookId"), "left")
  joinedDF.show()

  val finalAnalysis = joinedDF.withColumn("Mots/Ligne", $"mots"/$"lines")
                      .select("id", "mots", "lines", "Mots/Ligne")
  finalAnalysis.show()
  // On separe les lignes en mots (String => Array[String])
  // (Vu que logData est déjà un DataSet on peux appliquer un map directement
  // on peut donc separer les lignes en mots en utilisant selectExpr)
  val motsLigne = finalBooks.selectExpr("split(content, ' ') as mots")

  // Puis on explode les Arrays des mots pour avoir un Array avec tous les mots du texte
  // et on fais juste le count de cet array.
  val allMots = motsLigne.selectExpr("explode(mots) as mot")
  //allMots.show()
  val nbMots = allMots.count()

  // Calcul de la moyenne de mots par ligne
  val moyMots = nbMots/nbLignes

  // Calcul de la mediane de mots par ligne
  val aName = allMots.selectExpr("length(mot) as lg")
  val lignes = aName.orderBy("lg").select("lg").collect()
  val medMots = lignes((nbMots/2).toInt)
  
  //println(s"Il y a $nbLignes Lignes dans le texte")
  println(s"Il y a $nbMots Mots dans le texte")
  println(s"Il y a $nbLignes Lignes dans le texte")
  println(s"Il y a en moyenne $moyMots Mots par Ligne dans le texte")
  println(s"Il y a en mediane $medMots Mots par Ligne dans le texte")

  spark.stop()
}
