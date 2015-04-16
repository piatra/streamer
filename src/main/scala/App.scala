import java.io.PrintWriter
import java.util.concurrent.LinkedBlockingQueue

import twitter4j._

object App {
  val writequeue = new LinkedBlockingQueue[(String, String)]
  val readqueue = new LinkedBlockingQueue[(String, String)]

  object Util {
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey("2xxSiw42Ia3jSddvNzqaBVOv7")
      .setOAuthConsumerSecret("UWDP3MtEfalQf7AAauuxRMasDKPjUUHsB6CxR3TDNitEavLVvG")
      .setOAuthAccessToken("80282681-xCPKdvowdCTQblkemAKvf8uIa5WJelhxw6sOIkg1m")
      .setOAuthAccessTokenSecret("BPdFtPDrvUxcBD3hmBeo0g5RM0usaB3p6cZIBZa6Xk7fy")
      .build

    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status): Unit = {
        writequeue.put((status.getUser.getScreenName, status.getText))
      }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex: Exception) { ex.printStackTrace }
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    }
  }

  object StatusStreamer {
    val twitterStream = new TwitterStreamFactory(Util.config).getInstance

    def fetchTweets(keywords: Array[String]) {
      val filter = new FilterQuery()

      twitterStream.addListener(Util.simpleStatusListener)
      filter.language(Array("en"))
      filter.track(keywords)
      twitterStream.filter(filter)
    }

    def shutdown(): Unit = {
      writequeue.put("STOP", "STOP")

      twitterStream.cleanUp()
      twitterStream.shutdown()
    }
  }

  def main(args : Array[String]) {
    val prodThread = new Thread(new KafkaProducer(writequeue))
    prodThread.start()

    StatusStreamer.fetchTweets(Array("javascript", "nodejs", "scala", "python"))
    val topic: String = "test"
    val threads = 1
    val xmlOut = new PrintWriter("myfileout")
//
    val example = new KafkaConsumer(topic, readqueue)
    example.run(threads)

    Thread.sleep(2000)
    StatusStreamer.shutdown()
    prodThread.join()
    example.shutdown()

    println(readqueue.size() + " tweets to parse")

    val TweetParser = new TweetParser(readqueue, xmlOut)
    println("Print to file")
    TweetParser.printToFile(xmlOut)

    xmlOut.close()
  }
}