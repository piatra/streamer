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
    node.map(e => e.drop(2).head).groupBy(e => e).map(e => e._1 -> (e._2.length.toFloat / length))
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
    println(sentences)
    if (sentences != null && sentences.size() != 0) {
      weights = computeWeight(sentences.map { sentence =>
        val ne = new NodeExtractor(sentence.get(classOf[CollapsedDependenciesAnnotation]))
        ne.getAll().filter(isValid)
      }.flatten.toArray)
    }

    weights
  }

  def groupTweets(tweets: LinkedBlockingQueue[(String, String)]): Map[Int,Int] = {

    val weightedTweets: Iterable[Map[String, Float]] = tweets.map(tweetSentenceWeights)

    val simt = weightedTweets.map{ tweet =>
      val idx = weightedTweets.toSeq.indexOf(tweet)
      weightedTweets.map{ compareTweet =>
        val idy = weightedTweets.toSeq.indexOf(compareTweet)
        if (idx == idy)
          0
        else
          tweet.keySet.intersect(compareTweet.keySet).toArray.foldLeft(0.toFloat)((acc, e) => computeWeightAverage(
            acc, e, tweet, compareTweet
          ))
      }
    }

    val half = simt.map{lst =>
      var idx = -1
      lst.map{elem =>
        idx += 1
        (elem, idx)
      }.filter(e => e._1 == lst.max && e._1 > 0.15)
    }

    val adjencency = half.map{lst =>
      lst.foldLeft(List[Int]())((acc, weights) => acc :+ weights._2)
    }.zipWithIndex.map{elem =>
      elem._1 :+ elem._2
    }.map(e => e.sorted)

    adjencency.zipWithIndex.foldLeft(Map.empty[Int, Int])((acc, e) => {
      acc + (e._2 -> e._1.min)
    })
  }

  def printToFile(queue: LinkedBlockingQueue[(String, String)]): Unit = {
    val m = groupTweets(queue)
    println(m)
    val outFile = new PrintWriter("myfileout")
    outFile.println(m.map(e => (e._2, tweetList(e._1))).toList.toJson)
    outFile.flush()
    outFile.close()
  }

  def getCluster(key: Int, m: scala.collection.mutable.Map[Int,Int]): Int = {
    m.get(key) match {
      case Some(x) => if (x == key) x else getCluster(x, m)
      case None => key
    }
  }

  def computeWeightAverage(acc: Float, e: String, first: Map[String, Float], second: Map[String, Float]): Float = {
    first.get(e) match {
      case Some(x) => second.get(e) match {
        case Some(y) => acc + (x + y) / 2
        case _ => acc
      }
      case _ => acc
    }
  }
}