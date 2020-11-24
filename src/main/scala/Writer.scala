import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col

class Writer {
  var path = "C:\\Users\\gantosik\\IdeaProjects\\Quest1Scala-master\\src\\main\\Output\\"
  def write(clientDf:DataFrame, accountsDf:DataFrame, operationsDf:DataFrame, rateDf:DataFrame) {

    val rClientDf = clientDf
      .repartition(col("RegisterDate"))
      .write.format("parquet")
      .partitionBy("RegisterDate")
      .mode(SaveMode.Overwrite) parquet (path + "client.parquet")

    val rAccountDF = accountsDf
      .repartition(col("DateOpen"))
      .write.format("parquet")
      .partitionBy("DateOpen")
      .mode(SaveMode.Overwrite) parquet (path + "accounts.parquet")

    val rOperationsDf = operationsDf
      .repartition(col("DateOp"))
      .write.format("parquet")
      .partitionBy("DateOp")
      .mode(SaveMode.Overwrite) parquet (path + "operations.parquet")

    val rRateDf = rateDf
      .repartition(col("RateDate"))
      .write.format("parquet")
      .partitionBy("RateDate")
      .mode(SaveMode.Overwrite) parquet (path + "rate.parquet")
  }
}