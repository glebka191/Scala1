package showcases

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace}

//2020-11-01 2020-11-02
// 2020-11-03 2020-11-04

class FirstShowcase (){
  def showcase(bigDf:DataFrame): Unit ={
    val corporate_payments = bigDf
      .drop("ClientName", "Form", "RegisterDate", "DateOpen")
      .withColumn("ClientId", col("ClientId").cast("Int"))
      .withColumn("AccountDB", col("AccountDB").cast("Int"))
      .withColumn("AccountId", col("AccountId").cast("Int"))
      .withColumn("Amount", regexp_replace(col("Amount"), ",", ".").cast("Double"))
      .withColumn("CutoffDt", col("DateOp"))

    corporate_payments
      .orderBy("ClientId")
  }
}