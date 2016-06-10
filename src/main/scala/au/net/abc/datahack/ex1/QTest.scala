package au.net.abc.datahack.ex1

import java.util

import org.apache.http.HttpEntity
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import org.omg.CORBA.NameValuePair
import org.viz.lightning._

/**
 * Created by ojitha on 10/06/2016.
 */
object QTest {
  def main(args:Array[String]): Unit ={
    val lgn = Lightning(host="http://localhost:3000")

    lgn.post("/sessions/14/visualizations","test","rwar")
    lgn.line(Array(Array(1.0,1.0,2.0,3.0,9.0,20.0)))
    lgn.scatter(Array(1.0,2.0,3.0), Array(1.0,1.5,5.0))


    lgn.plot("line", Map("series" -> List(1,1,2,3,9,20)))
  }

}
