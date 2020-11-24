import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val reader = new Reader;
    reader.read(args(0),args(1),args(2),args(3))









    /*val sc = new SparkContext("local[*]", "Spark")
    val spark = SparkSession.builder.appName("Application").getOrCreate()
    val ClientDf = spark.read.csv(args(0))
    val AccountsDf = spark.read.csv(args(1))
    val OperationsDf = spark.read.csv(args(2))
    val RateDf = spark.read.csv(args(3))

    ClientDf.write.format("parquet")
      .mode(SaveMode.Overwrite)parquet("C:\\Users\\gantosik\\IdeaProjects\\untitled4\\src\\main\\Output\\client.parquet")

    AccountsDf.write.format("parquet")
      .mode(SaveMode.Overwrite)parquet("C:\\Users\\gantosik\\IdeaProjects\\untitled4\\src\\main\\Output\\accounts.parquet")

    OperationsDf.write.format("parquet")
      .mode(SaveMode.Overwrite)parquet("C:\\Users\\gantosik\\IdeaProjects\\untitled4\\src\\main\\Output\\operations.parquet")

    RateDf.write.format("parquet")
      .mode(SaveMode.Overwrite)parquet("C:\\Users\\gantosik\\IdeaProjects\\untitled4\\src\\main\\Output\\rate.parquet")
  */

  }
}