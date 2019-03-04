import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._ // mean(), sum()...


object Main {

  def replace(df: DataFrame,column: String, oldString: String, newString: String) : DataFrame = {
    df.withColumn(column, regexp_replace(col(column) , lit(oldString), lit(newString)))
  }

  def castDouble(df: DataFrame, s: String) : DataFrame = {
    df.withColumn(s, df(s).cast("double"))
  }

  def minMaxScaler(df: DataFrame, colName: String): DataFrame = {
    var colMax = df.agg(max(col(colName))).collect()(0)(0)
    var colMin = df.agg(min(col(colName))).collect()(0)(0)
    var colDiff = lit(colMax) - lit(colMin)
    df.withColumn(colName, (df(colName) - colMin) / colDiff)
  }

  def main(args: Array[String]) {
    // DataFrame immutable in Scala, so operations mean new DataFrames

    // Hide end warnings/infos at the end of build. (En local)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Initialize Session - Taking out .master("local[*]") not to overwrite
    val spark = SparkSession.builder().appName("CreditJobScala").getOrCreate()

    //Import data as dataframe
    val df = spark.read.option("header","true").option("delimiter",",").csv("s3n://creditstorage/credits.csv")

    // Rename and drop columns
    var dfs = df.withColumnRenamed("oldbalanceOrg", "oldBalanceOrig").withColumnRenamed("newbalanceOrig", "newBalanceOrig").withColumnRenamed("oldbalanceDest", "oldBalanceDest").withColumnRenamed("newbalanceDest", "newBalanceDest").drop("nameDest").drop("nameOrig")

    // Categorical to Numerical
    for (tuple <- Array(("CASH_OUT", "1"), ("PAYMENT", "2"), ("CASH_IN", "3"), ("TRANSFER", "4"), ("DEBIT", "5"))) {
      dfs = replace(dfs, "type", tuple._1, tuple._2)
    }

    for (s <- Array("step", "type", "amount", "oldBalanceOrig", "newBalanceOrig", "oldBalanceDest", "newBalanceDest", "isFraud", "isFlaggedFraud")) {
      dfs = castDouble(dfs, s)
    }

    for (col <- Array("step", "type", "amount", "oldBalanceOrig", "newBalanceOrig", "oldBalanceDest", "newBalanceDest", "isFlaggedFraud")) {
      println("Scaling: "+ col)
      dfs = minMaxScaler(dfs, col)
    }

    val reorderedColumnNames: Array[String] = Array("isFraud", "step", "type", "amount", "oldBalanceOrig", "newBalanceOrig", "oldBalanceDest", "newBalanceDest", "isFlaggedFraud")
    val result: DataFrame = dfs.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

    //
    //val Xtrain = dfs.drop("isFraud")
    //val Ytrain = dfs.select("isFraud")

    result.show(5)
    result.printSchema()

    result.write.format("csv").option("header", "false").save("s3n://creditstorage/oncemore")

    spark.close()
  }
}