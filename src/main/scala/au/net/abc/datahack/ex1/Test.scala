package au.net.abc.datahack.ex1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.Row;

/**
 * Created by ojitha on 9/06/2016.
 */
object Test {
  def main(args:Array[String]): Unit ={
    val logFile = "/Users/ojitha/aws/hack/test.txt"
    val conf = new SparkConf()
    conf.setAppName("Example")
    conf.setSparkHome("$SPARK_HOME")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile,2).cache()

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schemaString = "AF|BF"
    val schema =
      StructType(
        schemaString.split("\\|").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = logData.map(_.split("\\|")).map(p => Row(p(0), p(3)))
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.registerTempTable("logData")
    val results = sqlContext.sql("SELECT AF, BF FROM logData")

    results.map(t => "Name: " + t(1)).collect().foreach(println)
//    val lines = logData.take(2)
//    val fields =lines.foreach(e => e.split("\\|"))
//    print(fields.getClass)

  }

}
