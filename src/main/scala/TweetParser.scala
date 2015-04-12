import java.io.PrintWriter
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import edu.stanford.nlp.ling.{CoreAnnotations, IndexedWord}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedDependenciesAnnotation

import scala.collection.JavaConversions._

class TweetParser(tweets: LinkedBlockingQueue[(String, String)], out: PrintWriter) {

  def blacklist = List("DT", "CC", "PRP", "RT", "RB")
  def whitelist = List("USR", "LOCATION", "PERSON")

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

  def printWord(node: util.ArrayList[String]): Unit = {
    out.print("[ "+ node.mkString(" ") + " ]")
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

  def print() {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse")
    props.put("pos.model", "lib/models/gate-EN-twitter.model")
    val pipeline = new StanfordCoreNLP(props)

    println(tweets.size() + " tweets to parse")

    val weightedTweets: Iterable[Map[String, Float]] = tweets.map{ tweet =>
      var cleanTweet = formatTweet(tweet._2)
      out.println(tweet)
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

      // Add tweet user to the map
      weights += tweet._1 -> 1.toFloat
      weights
    }

    println(weightedTweets)

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

    println(simt)
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