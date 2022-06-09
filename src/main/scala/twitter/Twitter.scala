package twitter

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.twitter.TwitterSource

import java.io.{FileNotFoundException, PrintWriter}
import java.util.Properties
import scala.io.Source

object Twitter extends LazyLogging{

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

  import org.apache.flink.streaming.api.scala._

  def initTwitterProps(props: Properties): Properties = {

    val twitterProps = new Properties()
    twitterProps.setProperty(TwitterSource.CONSUMER_KEY, props.getProperty("CONSUMER_KEY") )
    twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, props.getProperty("CONSUMER_SECRET"))
    twitterProps.setProperty(TwitterSource.TOKEN, props.getProperty("TOKEN"))
    twitterProps.setProperty(TwitterSource.TOKEN_SECRET, props.getProperty("TOKEN_SECRET"))
    twitterProps
  }

  def getConfigs = {

    val url = getClass().getResource("app.properties")
    val properties: Properties = new Properties()

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    else {
      logger.error("properties file cannot be loaded at path ")
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    properties

  }

  def apply() = {
    val props = getConfigs
    props.list(System.out)
    println(props.getProperty("CONSUMER_KEY"))

    val streamSource = streamEnv.addSource(new TwitterSource(props))

    streamSource.print()

    streamEnv.execute("twitterStream")
  }

}