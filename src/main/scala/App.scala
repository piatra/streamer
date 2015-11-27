import java.util.concurrent.LinkedBlockingQueue

import controllers.Server.FinagleServer
import twitter4j._

object App {
  val rawTweetsQueue = new LinkedBlockingQueue[(String, String)]
  val topic: String = "javascript2"
  val threads = 1

  object Util {
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey("2xxSiw42Ia3jSddvNzqaBVOv7")
      .setOAuthConsumerSecret("UWDP3MtEfalQf7AAauuxRMasDKPjUUHsB6CxR3TDNitEavLVvG")
      .setOAuthAccessToken("80282681-xCPKdvowdCTQblkemAKvf8uIa5WJelhxw6sOIkg1m")
      .setOAuthAccessTokenSecret("BPdFtPDrvUxcBD3hmBeo0g5RM0usaB3p6cZIBZa6Xk7fy")
      .build
    val prodThread = new KafkaProducer(topic)

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
    StatusStreamer.fetchTweets(Array("javascript", "python", "clojure", "erlang"))
    println("Wait to fetch some tweets...")
    Thread.sleep(180000)
    println("Resuming")
    val server = new FinagleServer
    server.serve()
    println("server started")
    println("start the consumer")
    val example = new KafkaConsumer(topic, rawTweetsQueue)
    example.run(threads)
    println("consumer started")
    println("start tweet parser")
    new Thread(new TweetParser(rawTweetsQueue)).start()
    println("started")
  }
}