import showcases.FirstShowcase
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

class Reader(){
  def read(client:String, accounts:String, operations:String, rate:String) ={
    val sc = new SparkContext("local[*]", "Spark")
    val spark = SparkSession.builder.appName("Application").getOrCreate()

    val clientDf = spark
      .read
      .option("delimiter",";")
      .option("header", "true")
      .option("encoding","Windows-1251")
      .csv(client)

    val accountsDf = spark.read
      .option("delimiter",";")
      .option("header", "true")
      .option("encoding","Windows-1251")
      .csv(accounts)

    val operationsDf = spark.read
      .option("delimiter",";")
      .option("header", "true")
      .option("encoding","Windows-1251")
      .csv(operations)

    val rateDf = spark.read
      .option("delimiter",";")
      .option("header", "true")
      .option("encoding","Windows-1251")
      .csv(rate)



    val write = new Writer().write(clientDf,accountsDf,operationsDf,rateDf);



    val maxRateDate = rateDf.agg(max("RateDate"))
    val tempDF = operationsDf
      .join(rateDf, Seq("Currency"), "inner")
      .where(rateDf("RateDate") === maxRateDate.head().apply(0))
      .drop("RateDate", "Comment", "AccountCR", "DateOp", "Amount", "AccountDB")
      .distinct()

    var bigDF = clientDf
      .join(
        accountsDf,
        Seq("ClientId"),
        "inner"
      )
    bigDF = bigDF
      .join(
        operationsDf,
        operationsDf.col("AccountDB") === bigDF.col("AccountId"),
        "inner"
      )
    bigDF = bigDF
      .join(
        tempDF,
        Seq("Currency"),
        "inner"
      )

    val showcase = new FirstShowcase().showcase(bigDF);

  }
}