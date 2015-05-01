import java.util.concurrent.LinkedBlockingQueue

import kmeans.KMeans
import twitter4j._

object App {
  val readqueue = new LinkedBlockingQueue[(String, String)]

  object Util {
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey("2xxSiw42Ia3jSddvNzqaBVOv7")
      .setOAuthConsumerSecret("UWDP3MtEfalQf7AAauuxRMasDKPjUUHsB6CxR3TDNitEavLVvG")
      .setOAuthAccessToken("80282681-xCPKdvowdCTQblkemAKvf8uIa5WJelhxw6sOIkg1m")
      .setOAuthAccessTokenSecret("BPdFtPDrvUxcBD3hmBeo0g5RM0usaB3p6cZIBZa6Xk7fy")
      .build
    val prodThread = new KafkaProducer()

    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status): Unit = {
        prodThread.putTweet(status.getUser.getScreenName, status.getText)
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
  }

  def main(args : Array[String]) {

    var points = List(
      Map("prices" -> 0.3f, "went" -> 0.3f, "down" -> 0.3f),
      Map("Railroad" -> 0.3f, "kings" -> 0.3f, "essentially" -> 0.3f, "created"->0f, "the"->0f, "law"->0f),
      Map("Worker" -> 0.3f, "lost" -> 0.3f, "individuality" -> 0.3f, "and"->0f, "independence"->0f),
      Map("Power" -> 0.3f, "of" -> 0.3f, "trusts" -> 0.3f),
      Map("Social" -> 0.3f, "gospel" -> 0.3f, "of" -> 0.3f, "wealth"->0f, "responsibility"->0f, "of"->0f, "the"->0f, "rich"->0f),
      Map("Returning" -> 0.3f, "government" -> 0.3f, "to" -> 0.3f, "the"->0f, "hands"->0f, "of"->0f, "the"->0f, "common"->0f, "man"->0f),
      Map("Workers" -> 0.3f, "should" -> 0.3f, "be" -> 0.3f, "treated"->0f, "as"->0f, "equals"->0f, "compared"->0f, "to"->0f, "everyone"->0f, "else"->0f),
      Map("An" -> 0.3f, "oil" -> 0.3f, "company" -> 0.3f, "ruined"->0f, "by"->0f, "Rockefeller"->0f),
      Map("Departament" -> 0.3f, "store" -> 0.3f),
      Map("Female" -> 0.3f, "typists" -> 0.3f)
    )

    var tdidf = new KMeans(points)
    tdidf.cluster(3)


    //      val server = new FinagleServer
//      server.serve()
//      StatusStreamer.fetchTweets(Array("javascript", "python"))
//
//      val topic: String = "javascript"
//      val threads = 1
//
//      val example = new KafkaConsumer(topic)
//      example.run(threads)
//      Thread.sleep(1000)
//      example.shutdown()
  }
}