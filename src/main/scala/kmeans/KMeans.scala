package kmeans

import scala.util.Random

import TfIdf.TfIdf

class KMeans(points: Iterable[Array[String]]) {
  val tfidf = new TfIdf(points)

  def cluster(nc: Int): (Iterable[Int], Double) = {
    var maxIterations = 20
    var initialClusters = getRandomPoints(tfidf.vectorspace, nc)
    var ok: Boolean = true
    var assignment: Vector[Int] = Vector()
    while (ok && maxIterations > 0) {
      assignment = tfidf.vectorspace.map(assignToClosest(initialClusters))

      // compute cluster average (centroid)
      val step = initialClusters.map { cluster =>
        val p = assignment.zipWithIndex.filter(e => initialClusters.indexOf(cluster) == e._1)
                  .map(e => (e._1, tfidf.vectorspace(e._2))).map(_._2)
        p.transpose.map(_.sum).map(e => e / p.length)
      }

      val d = initialClusters.zip(step).par.map(pairOfLists => delta(pairOfLists)).map(_.sum).sum
      if (d == 0) {
        ok = false
      } else {
        initialClusters = step
      }
      maxIterations = maxIterations - 1
    }
    val similarity = assignment.zipWithIndex
                        .par.map(e => tfidf.cosineSimilarity(initialClusters(e._1), tfidf.vectorspace(e._2))).sum

    (assignment, similarity)
  }

  def clusterIterations(it: Int): Iterable[Int] = {
    val attempts: Iterable[(Iterable[Int], Double)] = List.fill(it)(this.cluster(Math.floor(Math.sqrt(points.size / 2)).toInt))
    attempts.maxBy(_._2)._1
  }

  def delta(xs: (Vector[Double], Vector[Double])): Vector[Double] = {
    try {
      xs._1.zipWithIndex.map(e => Math.abs(e._1 - xs._2(e._2)))
    } catch {
      case _: Throwable => Vector.fill(xs._1.length)(0)
    }
  }

  def getRandomPoints(vectorspace: Vector[Vector[Double]], n: Int): Vector[Vector[Double]] = {
    Random.shuffle(vectorspace).take(n)
  }

  def assignToClosest(clusters: Vector[Vector[Double]])(point: Vector[Double]): Int = {
    val distances = clusters.map(e => tfidf.cosineSimilarity(e, point))
    distances.indexOf(distances.max)
  }
}