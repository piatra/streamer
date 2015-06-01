import java.io.PrintWriter
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import edu.stanford.nlp.ling.{CoreAnnotations, IndexedWord}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedDependenciesAnnotation
import kmeans.KMeans
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConversions._

class TweetParser(tweetQueue: LinkedBlockingQueue[(String, String)]) extends Runnable {

  def blacklist = List("DT", "CC", "PRP", "RT", "RB", "WP", "TO", "IN", "PRP")
  def whitelist = List("USR", "LOCATION", "PERSON", "NNP")
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse")
  props.put("pos.model", "lib/models/gate-EN-twitter-fast.model")
  val pipeline = new StanfordCoreNLP(props)
  val queue: LinkedBlockingQueue[(String, String)] = new LinkedBlockingQueue[(String, String)]()
  val parsedTweetsQueue: LinkedBlockingQueue[(String, String)] = new LinkedBlockingQueue[(String, String)]()
  val prodThread = new KafkaProducer("parsed")

  def run() {
    println("tweet parser started")
    while (tweetQueue.size > 0) {
      // can return null if the queue is empty
      var hasElems = true
      val elem: (String, String) = tweetQueue.poll(1, java.util.concurrent.TimeUnit.SECONDS)
      println(elem)
      println("tweetqueue size " + tweetQueue.size)
      elem match {
        case (a: String, b: String) => hasElems = true
        case _ => hasElems = false
      }

      if (hasElems) {
        queue.put(elem)
        if (queue.size > 0) {
          println("parse " + queue.size + " tweets")
          printToFile(queue)
        }
      }
    }
    println("Done")
  }

  def isValid(tokens: util.ArrayList[String]): Boolean = {
    if (blacklist.indexOf(tokens.head) >= 0) {
      return false
    }
    val level = Integer.parseInt(tokens.last)
    if (level > 2) {
      if (whitelist.indexOf(tokens.head) == -1) {
        return false
      }
    }

    true
  }

  def computeWeight(node: Array[util.ArrayList[String]]): List[String] = {
    node
      .map(e => e.drop(2).head).groupBy(e => e)
      .map(e => e._1).toList
  }

  def extractWord(node: IndexedWord): List[String] = {
    List(node.tag(), node.ner(), node.originalText())
  }

  def formatTweet(tweet: String): String = {
    var httpRegex = """http[:\/0-9a-zA-Z\.#%&=\-~]+""".r
    // remove pound sign from hashtags
    var buf = tweet.replaceAll("#", "")
    // remove all links
    buf = httpRegex.replaceAllIn(buf, "")
    // remove all non-alphanumeric characters
    buf.replaceAll("[^A-Za-z0-9 ]", "");
  }

  def tweetSentenceWeights(tweet: (String, String)): List[String] = {

    val cleanTweet = formatTweet(tweet._2)
    val document = new Annotation(cleanTweet)
    pipeline.annotate(document)

    val sentences = document.get(classOf[CoreAnnotations.SentencesAnnotation])
    var weights = List[String]()
    if (sentences != null && sentences.size() != 0) {
      weights = computeWeight(sentences.map { sentence =>
        val ne = new NodeExtractor(sentence.get(classOf[CollapsedDependenciesAnnotation]))
        ne.getAll().filter(isValid)
      }.flatten.toArray)
    }

    weights
  }

  def kmeansGrouping(queue: LinkedBlockingQueue[(String, String)]): List[Int] = {
    println("group")
    val weightedTweets: List[List[String]] = queue.map(tweetSentenceWeights).toList
    println("weighted tweets")
    println(weightedTweets)
    weightedTweets.foreach(e => prodThread.putTweet("parsed", e.mkString(",")))

    println("get all parsed tweets")
    val syncConsumer = new SyncKafkaConsumer("parsed", parsedTweetsQueue)
    syncConsumer.getAll()
    println("got all parsed tweets")

    val listOfParsedTweets = parsedTweetsQueue.map(e => e._2.split(",").toList).toList
    println(listOfParsedTweets)
    println("KMeans clustering")

    val kmeans = new KMeans(listOfParsedTweets)
    kmeans.clusterIterations(3)
  }

  def printToFile(queue: LinkedBlockingQueue[(String, String)]): Unit = {
    val m = kmeansGrouping(queue).zip(queue)
    println("Write to file")
    val outFile = new PrintWriter("output.json")
    outFile.println(m.toJson)
    outFile.flush()
    outFile.close()
  }
}