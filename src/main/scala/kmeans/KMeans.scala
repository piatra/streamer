package kmeans

import TfIdf.TfIdf
import scala.util.Random

class KMeans(points: List[List[String]]) {
  val tfidf = new TfIdf(points)

  def cluster(nc: Int): (List[Int], Double) = {
    var maxIterations = 20
    var initialClusters = getRandomPoints(tfidf.vectorspace, nc)
    var ok: Boolean = true
    var assignment: List[Int] = Nil
    while (ok && maxIterations > 0) {
      assignment = tfidf.vectorspace.map(assignToClosest(initialClusters))

      // compute cluster average (centroid)
      val step = initialClusters.map { cluster =>
        val p = assignment.zipWithIndex.map(e => (e._1, tfidf.vectorspace(e._2)))
          .filter(e => initialClusters.indexOf(cluster) == e._1).map(_._2)
        p.transpose.map(_.sum).map(e => e / p.size)
      }

      val d = initialClusters.zip(step).map(pairOfLists => delta(pairOfLists)).map(_.sum).sum
      if (d == 0) {
        ok = false
      } else {
        initialClusters = step
      }
      maxIterations = maxIterations - 1
    }
    val similarity = assignment.zipWithIndex
                                .map(e => tfidf.cosineSimilarity(initialClusters(e._1), tfidf.vectorspace(e._2))).sum
    (assignment, similarity)
  }

  def delta(xs: (List[Double], List[Double])): List[Double] = {
    xs._1.zipWithIndex.map(e => Math.abs(e._1 - xs._2(e._2)))
  }

  def getRandomPoints(vectorspace: List[List[Double]], n: Int): List[List[Double]] = {
    Random.shuffle(vectorspace).take(n)
  }

  def assignToClosest(clusters: List[List[Double]])(point: List[Double]): Int = {
    val distances = clusters.map(e => tfidf.cosineSimilarity(e, point))
    distances.indexOf(distances.max)
  }
}