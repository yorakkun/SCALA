import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SimpleApp extends App {
  val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
  import spark.implicits._

  // Load the text file into a DataFrame
  val logFile = "./data/testing.txt" // Adjust the path as needed
  val textData = spark.read.text(logFile).as[String]

  // Identify lines containing "ISBN" as new book starts and assign IDs
  val withBookStarts = textData.withColumn("isNewBook", lower($"value").contains("isbn"))
  val bookIds = withBookStarts.withColumn("bookId", sum(when($"isNewBook", 1).otherwise(0)).over(Window.orderBy(monotonically_increasing_id())))

  // Filter out the ISBN lines if you want only content, adjust as needed
  val cleanedData = bookIds.filter(!$"isNewBook")

  // Group by 'bookId' and aggregate contents
  val books = cleanedData.groupBy($"bookId").agg(collect_list($"value").as("content"))

  // Convert the aggregated content into a single string per book and assign IDs
  val finalBooks = books.withColumn("id", monotonically_increasing_id())
                        .withColumn("content", concat_ws(" ", $"content"))
                        .select("id", "content") // Select only the necessary columns

  // Show results
  finalBooks.show(true)

  // Convert to Dataset of a case class if needed for better type safety
  finalBooks.as[(Long, String)].show(true)

  spark.stop()
}
