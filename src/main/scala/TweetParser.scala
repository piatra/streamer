import java.io.PrintWriter
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import edu.stanford.nlp.ling.{CoreAnnotations, IndexedWord}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedDependenciesAnnotation

import scala.collection.JavaConversions._

import spray.json._
import DefaultJsonProtocol._ // if you don't supply your own Protocol (see below)

class TweetParser() {

  def blacklist = List("DT", "CC", "PRP", "RT", "RB")
  def whitelist = List("USR", "LOCATION", "PERSON")
  var tweetList = List[(String, String)]()
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse")
  props.put("pos.model", "lib/models/gate-EN-twitter.model")
  val pipeline = new StanfordCoreNLP(props)
  val queue: LinkedBlockingQueue[(String, String)] = new LinkedBlockingQueue[(String, String)]()

  def queueTweet(tweet: (String, String)): Unit = {
    queue.put(tweet)
    if (queue.size() % 10 == 0) {
      printToFile(queue)
    }
  }

  def isValid(tokens: util.ArrayList[String]): Boolean = {
    if (blacklist.indexOf(tokens.head) >= 0) {
      return false
    }
    val level = Integer.parseInt(tokens.last)
    if (level > 2) {
      val ner = tokens.drop(1).head
      if (whitelist.indexOf(ner) == -1) {
        return false
      }
    }

    return true
  }

  def computeWeight(node: Array[util.ArrayList[String]]): Map[String, Float] = {
    val length = node.length
    node
      .map(e => e.drop(2).head).groupBy(e => e)
      .map(e => e._1 -> (e._2.length.toFloat / length))
  }

  def extractWord(node: IndexedWord): List[String] = {
    List(node.tag(), node.ner(), node.originalText())
  }

  def formatTweet(tweet: String): String = {
    var httpRegex = """http[:\/0-9a-zA-Z\.#%&=\-~]+""".r
    var buf = tweet.replaceAll("#", "")
    buf = httpRegex.replaceAllIn(buf, "")
    buf
  }

  def tweetSentenceWeights(tweet: (String, String)): Map[String, Float] = {

    tweetList = tweetList :+ tweet

    val cleanTweet = formatTweet(tweet._2)
    val document = new Annotation(cleanTweet)
    pipeline.annotate(document)

    val sentences = document.get(classOf[CoreAnnotations.SentencesAnnotation])
    var weights = Map[String, Float]()
    if (sentences != null && sentences.size() != 0) {
      weights = computeWeight(sentences.map { sentence =>
        val ne = new NodeExtractor(sentence.get(classOf[CollapsedDependenciesAnnotation]))
        ne.getAll().filter(isValid)
      }.flatten.toArray)
    }

    weights
  }

  def groupTweets(tweets: LinkedBlockingQueue[(String, String)]): Iterable[(Int,Int,Float)] = { // ID, PAIR, MATCH

    val weightedTweets: Iterable[Map[String, Float]] = tweets.map(tweetSentenceWeights)

    val matches = for { x <- weightedTweets
        y <- weightedTweets
        idx = weightedTweets.toSeq.indexOf(x)
        idy = weightedTweets.toSeq.indexOf(y)
        if (idx != idy)}
      yield (idx, idy, x.keySet.intersect(y.keySet).toList
        .map(e => (x.getOrElse(e, 0.toFloat) + y.getOrElse(e, 0.toFloat)) / 2).sum)

//    matches.filter(e => e._3 > 0).foreach{ e =>
//      println(weightedTweets.toList(e._1))
//      println(weightedTweets.toList(e._2))
//      println()
//    }

    matches.filter(e => e._3 > 0)
  }

  def printToFile(queue: LinkedBlockingQueue[(String, String)]): Unit = {
    val m = groupTweets(queue)
    val outFile = new PrintWriter("myfileout")
    outFile.println(m.map(e => (e._1, e._2, e._3, tweetList(e._1), tweetList(e._2))).toList.toJson)
    outFile.flush()
    outFile.close()
  }

  def getCluster(key: Int, m: scala.collection.mutable.Map[Int,Int]): Int = {
    m.get(key) match {
      case Some(x) => if (x == key) x else getCluster(x, m)
      case None => key
    }
  }
}