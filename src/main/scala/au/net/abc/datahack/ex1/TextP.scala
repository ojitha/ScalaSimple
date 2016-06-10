package au.net.abc.datahack.ex1

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ojitha on 9/06/2016.
 */
object TextP {
  def main(args: Array[String]): Unit ={
    val logFile = "/Users/ojitha/aws/hack/data.txt"
    val conf = new SparkConf()
    conf.setAppName("Example")
    conf.setSparkHome("$SPARK_HOME")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile,2).cache()

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schemaString = "type|action"
    val schema =
      StructType(
        schemaString.split("\\|").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = logData.map(_.split("\\|")).map(p => Row(p(53), p(54)))
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.registerTempTable("logData")
    val results = sqlContext.sql("SELECT type, action FROM logData")

    results.map(t => "Name: " + t(0)).collect().foreach(println)


  }
}
