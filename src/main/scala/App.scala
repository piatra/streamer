import java.util.concurrent.LinkedBlockingQueue

import twitter4j._

object App {
  val queue = new LinkedBlockingQueue[(String, String)]

  object Util {
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey("2xxSiw42Ia3jSddvNzqaBVOv7")
      .setOAuthConsumerSecret("UWDP3MtEfalQf7AAauuxRMasDKPjUUHsB6CxR3TDNitEavLVvG")
      .setOAuthAccessToken("80282681-xCPKdvowdCTQblkemAKvf8uIa5WJelhxw6sOIkg1m")
      .setOAuthAccessTokenSecret("BPdFtPDrvUxcBD3hmBeo0g5RM0usaB3p6cZIBZa6Xk7fy")
      .build

    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status): Unit = {
        queue.put((status.getUser.getScreenName, status.getText))
      }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex: Exception) { ex.printStackTrace }
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    }
  }

  object StatusStreamer {
    def getTweets(keywords: Array[String]) {
      val twitterStream = new TwitterStreamFactory(Util.config).getInstance
      val filter = new FilterQuery()

      twitterStream.addListener(Util.simpleStatusListener)
      filter.language(Array("en"))
      filter.track(keywords)
      twitterStream.filter(filter)
      Thread.sleep(10000)

      queue.put("STOP", "STOP")
      twitterStream.cleanUp
      twitterStream.shutdown
    }
  }

  def main(args : Array[String]) {
    val prodThread = new Thread(new KafkaProducer(queue))
    prodThread.start()

    StatusStreamer.getTweets(Array("Justin Bieber"))
    prodThread.join()
  }
}