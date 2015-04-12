import java.io.PrintWriter
import java.util
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import edu.stanford.nlp.ling.{CoreAnnotations, IndexedWord}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedDependenciesAnnotation

import scala.collection.JavaConversions._

class TweetParser(tweets: LinkedBlockingQueue[(String, String)], out: PrintWriter) {

  def blacklist = List("DT", "CC", "PRP")
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

    true
  }

  def printWord(node: util.ArrayList[String]): Unit = {
    out.print("[ "+ node.mkString(" ") + " ]")
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

    tweets.foreach { tweet =>
      var cleanTweet = formatTweet(tweet._2)
      out.println(tweet._2)
      out.println(cleanTweet)
      out.print("\n[")
      val document = new Annotation(cleanTweet)
      pipeline.annotate(document)

      val sentences = document.get(classOf[CoreAnnotations.SentencesAnnotation])
      if (sentences != null && sentences.size() != 0) {
        sentences.foreach { sentence =>
          println(sentence)
          val tokens = sentence.get(classOf[CollapsedDependenciesAnnotation])

          val ne = new NodeExtractor(tokens)
          println(ne.getAll())

          ne.getAll().filter(isValid).foreach(printWord)
        }
      }
      out.print("]\n\n\n")
    }
  }
}