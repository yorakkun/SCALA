import org.apache.spark.sql.SparkSession

object SimpleApp extends App {
    val logFile = "./data/testing.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    var logData = spark.read.textFile(logFile).cache()

    val nbLignes = logData.count()

    // On separe les lignes en mots (String => Array[String])
    // (Vu que logData est déjà un DataSet on peux appliquer un map directement
    // on peut donc separer les lignes en mots en utilisant selectExpr)
    val motsLigne = logData.selectExpr("split(line, ' ') as mots")
    //motsLigne.show()

    // Puis on explode les Arrays des mots pour avoir un Array avec tous les mots du texte
    // et on fais juste le count de cet array.
    val nbMots = motsLigne.selectExpr("explode(mots) as mot").count()

    // Calcul de la moyenne de mots par ligne
    val moyMots = nbMots/nbLignes

    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    //println(s"Lines with a: $numAs, Lines with b: $numBs")

    println(s"Il y a $nbLignes Lignes dans le texte")
    println(s"Il y a $nbMots Mots dans le texte")
    println(s"Il y a en moyenne $moyMots Mots par Ligne dans le texte")
    spark.stop()
}
