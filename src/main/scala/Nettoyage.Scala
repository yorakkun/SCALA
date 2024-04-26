import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Nettoyage {
    def ClearData(spark: SparkSession): DataFrame = {
        val logFile = "./data/testing.txt" 
        val logData = spark.read.textFile(logFile)

        // Replace punctuation with empty characters
        val cleanData = logData.withColumn("clean_value", regexp_replace(logData("value"), "[^a-zA-Z0-9\\s]", ""))

        // Filter out empty lines
        val filteredData = cleanData.filter(col("clean_value") =!= "")

        return filteredData
    }
}