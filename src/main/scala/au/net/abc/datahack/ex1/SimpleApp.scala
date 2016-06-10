package au.net.abc.datahack.ex1

import java.io.PrintWriter
import java.util
import java.util.Calendar

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.omg.CORBA.NameValuePair

import scala.reflect.io.File

/**
 * Created by ojitha on 5/06/2016.
 */
object SimpleApp {
  def main(args: Array[String]): Unit ={

    val logFile = "/Users/ojitha/aws/hack/log/test.csv"
    val conf = new SparkConf()
    conf.setAppName("Example")
    conf.setSparkHome("$SPARK_HOME")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = ssc.socketTextStream("localhost", 9000, StorageLevel.MEMORY_AND_DISK_SER)

//    val schemaString = "type|action"
//    val schema =
//      StructType(
//        schemaString.split("\\|").map(fieldName => StructField(fieldName, StringType, true)))
//
//    val fields = lines.flatMap(_.split("\\|"))



    lines.foreachRDD{ rdd => if (rdd.count() > 0) {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to DataFrame
      val df = rdd.map(w => {
        val words = w.split("\\|")
        Record(words(0), words(35), words(5), words(53), words(54), words(55))
      }).toDF()


      // Register as table
      df.registerTempTable("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select * from words")

      //var result = ""

      wordCountsDataFrame.show(false)
//      val rowList = wordCountsDataFrame.collectAsList()
//      for ( i <- 0 to rowList.size()-1){
//        result = result + rowList.get(i).toString()
//        result = result + "\n"
//      }
//      println(result)

    }

    }

    ssc.start()
    ssc.awaitTermination()
  }
  case class Record(f1: String, f2:String,f3:String, f4: String, f5:String,f6:String)

  def doPost(str:String):Unit ={
    val url = "http://192.168.0.126:8888";

    val post = new HttpPost(url)
    post.addHeader("appid","YahooDemo")
    post.addHeader("query","umbrella")
    post.addHeader("results","10")

    val client = new DefaultHttpClient()
    val params = client.getParams
    params.setParameter("foo", "bar")

    val nameValuePairs = new util.ArrayList[NameValuePair](1)

    //    nameValuePairs.add(0,new BasicNameValuePair("registrationid", "123456789"));
    //    nameValuePairs.add(new BasicNameValuePair("accountType", "GOOGLE"));
    //    post.setEntity(new UrlEncodedFormEntity()(nameValuePairs));

    post.setEntity(new StringEntity("fuysyf"))
    println(" before post")
    // send the post request
    val response = client.execute(post)
    println("--- HEADERS ---")
    response.getAllHeaders.foreach(arg => println(arg))
  }
}
