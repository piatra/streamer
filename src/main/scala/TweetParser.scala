import java.io.PrintWriter
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import edu.stanford.nlp.ling.{CoreAnnotations, IndexedWord}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraph
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedDependenciesAnnotation
import scala.collection.JavaConversions._

class TweetParser(tweets: LinkedBlockingQueue[(String, String)], out: PrintWriter) {
  def printTree(root: IndexedWord, out: PrintWriter, tokens: SemanticGraph, level: Int): Unit = {
    val children = tokens.getChildList(root)
    for (child <- children) {
      var buf = "[" + child.originalText() + " " + child.ner() + " " + child.tag() + " " + level  + "] "
      out.print(buf)
      printTree(child, out, tokens, level + 1)
    }
  }

  def formatTweet(tweet: String): String = {
    var httpRegex = """http[:\/0-9a-zA-Z\.#%&=\-~]+""".r
    var buf = tweet.replaceAll("#", "")
    buf = httpRegex.replaceAllIn(buf, "")
    return buf
  }

  def print() {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
    val pipeline = new StanfordCoreNLP(props)

    tweets.foreach { tweet =>
      var cleanTweet = formatTweet(tweet._2)
      out.println(tweet._2)
      out.println(cleanTweet)
      out.print("\n[")
      val document = new Annotation(cleanTweet)
      pipeline.annotate(document)

      val sentences = document.get(classOf[CoreAnnotations.SentencesAnnotation])
      if (sentences != null && !sentences.isEmpty()) {
        val tokens = sentences.get(0).get(classOf[CollapsedDependenciesAnnotation])
        printTree(tokens.getFirstRoot, out, tokens, 0)
      }
      out.print("]\n\n\n")
    }
  }
}