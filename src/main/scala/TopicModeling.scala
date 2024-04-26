import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object TopicModeling {
  
    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

    def performLDA(spark: SparkSession, data: DataFrame): Unit = {
    import spark.implicits._

    // Assurez-vous que data a une colonne "tokens" qui contient les mots tokenisés
    val cvModel: CountVectorizerModel = new CountVectorizer()
        .setInputCol("tokens")
        .setOutputCol("features")
        .setVocabSize(10000) // Taille du vocabulaire
        .setMinDF(2) // mots apparaissant dans au moins 2 documents
        .fit(data)

    val vectorizedData = cvModel.transform(data)

    // Configuration de l'algorithme LDA
    val lda = new LDA()
        .setK(10) // Nombre de topics
        .setMaxIter(10) // Nombre d'itérations de convergence

    // Entraînement du modèle LDA
    val ldaModel = lda.fit(vectorizedData)

    // Récupération des topics et des mots associés
    val topics: DataFrame = ldaModel.describeTopics(5) // Nombre de mots par topic

    // Affichage des topics
    topics.show(false)
    }
}