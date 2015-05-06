package TfIdf

class TfIdf(tweets: List[List[String]]) {
  private val documentsSize = tweets.map(e => e.size).sum
  private val globalWords = tweets.flatten
  val vectorspace: List[List[Double]] = tweets.map{tweet =>
    globalWords.map{word =>
      this.termFrequency(tweet, word) * this.inverseDocumentFrequency(word)
    }
  }

  def termFrequency(tweet: List[String], term: String): Double = {
    this.noOfOccurrences(tweet, term) / tweet.size
  }

  def noOfOccurrences(tweet: List[String], term: String): Float = {
    tweet.foldLeft(0)((acc, e) => if (e.equalsIgnoreCase(term)) acc + 1; else acc)
  }

  def inverseDocumentFrequency(term: String): Double = {
    Math.log(documentsSize / tweets.foldLeft(0f)((acc, e) => acc + this.noOfOccurrences(e, term)))
  }

  def cosineSimilarity(a: List[Double], b: List[Double]): Float = {
    val productAndMagnitude = a.zip(b).map(e => (e._1 * e._2, Math.pow(e._1, 2), Math.pow(e._2, 2)))
      .foldLeft((0d, 0d, 0d))((acc, e) => (acc._1 + e._1, acc._2 + e._2, acc._3 + e._3))

    val r = productAndMagnitude._1.toFloat / (Math.sqrt(productAndMagnitude._2) * Math.sqrt(productAndMagnitude._3)).toFloat
    r match {
      case Double.NaN => 0
      case _ => r
    }
  }

}
