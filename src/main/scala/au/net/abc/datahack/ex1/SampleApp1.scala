package au.net.abc.datahack.ex1

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ojitha on 5/06/2016.
 */
object SimpleApp1 {
  def main(args: Array[String]): Unit ={
    //val logFile = "/Users/ojitha/spark-1.6.1-bin-hadoop2.6/README.md"
    val conf = new SparkConf()
    conf.setAppName("Example")
    conf.setSparkHome("$SPARK_HOME")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = ssc.socketTextStream("localhost", 9000, StorageLevel.MEMORY_AND_DISK_SER)

    val schemaString = "type|action"
    val schema =
      StructType(
        schemaString.split("\\|").map(fieldName => StructField(fieldName, StringType, true)))

    val fields = lines.flatMap(_.split("\\|"))

    lines.foreachRDD{ rdd =>
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to DataFrame
      val df = rdd.toDF()

      // Register as table
      df.registerTempTable("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select * from words")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }
  case class Record(word: String)
}
