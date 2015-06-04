package kmeans

import TfIdf.TfIdf

import scala.util.Random

class KMeans(points: Iterable[Array[String]]) {
  val tfidf = new TfIdf(points)

  def cluster(nc: Int): (Iterable[Int], Double) = {
    var maxIterations = 20
    var initialClusters = getRandomPoints(tfidf.vectorspace, nc)
    var ok: Boolean = true
    var assignment: Iterable[Int] = Nil
    while (ok && maxIterations > 0) {
      assignment = tfidf.vectorspace.map(assignToClosest(initialClusters))

      // compute cluster average (centroid)
      val step = initialClusters.map { cluster =>
        val p = assignment.zipWithIndex.filter(e => initialClusters.indexOf(cluster) == e._1)
                  .map(e => (e._1, tfidf.vectorspace(e._2))).map(_._2)
        p.transpose.map(_.sum).map(e => e / p.size).toArray
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

  def clusterIterations(it: Int): Iterable[Int] = {
    val attempts: Iterable[(Iterable[Int], Double)] = List.fill(it)(this.cluster(Math.floor(Math.sqrt(points.size / 2)).toInt))
    attempts.maxBy(_._2)._1
  }

  def delta(xs: (Array[Double], Array[Double])): Array[Double] = {
    try {
      xs._1.zipWithIndex.map(e => Math.abs(e._1 - xs._2(e._2)))
    } catch {
      case _: Throwable => Array.fill(xs._1.length)(0)
    }
  }

  def getRandomPoints(vectorspace: Array[Array[Double]], n: Int): Array[Array[Double]] = {
    Random.shuffle(vectorspace.toVector).take(n).toArray
  }

  def assignToClosest(clusters: Array[Array[Double]])(point: Array[Double]): Int = {
    val distances = clusters.map(e => tfidf.cosineSimilarity(e, point))
    distances.indexOf(distances.max)
  }
}