package kmeans

import scala.util.Random

import TfIdf.TfIdf

class KMeans(points: Vector[Vector[String]]) {
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
    val m = assignment.zip(tfidf.vectorspace).groupBy(e => e._1)
    val termsPerCluster = m.map {
      e => e._2.foldLeft(Vector.fill(e._2.head._2.size){0.0d}) {
        (acc, el) => {
          acc.zip(el._2).map(e => e._1 + e._2)
        }
      }
    }.toVector

    val silhouette = m.foreach {
      e => {

        // compute purity
        if (e._1 < termsPerCluster.size) {
          val aggregatedCluster = termsPerCluster(e._1)
          val purity = e._2.map(e => e._2).map(e => e.zip(aggregatedCluster).map(e => if (e._2 == 0) 1 else if (e._1 != 0) 1 else 0).sum)
                           .map(e => if (e > termsPerCluster.size / 2) 1 else 0).sum
          println("purity in cluster " + e._1 + ": " + purity + "/" + e._2.size)
        } else {
          println("purity: 0")
        }


        // random index
        val r = scala.util.Random
        val ria = r.nextInt(e._2.length)

        // as
        val seed = e._2(ria)._2
        val xs1  = e._2.filter(_._2 != seed)
        val as   = xs1.foldLeft(0.0d) { (acc, el) => acc + tfidf.cosineSimilarity(seed, el._2) }

        // bs
        var rib = r.nextInt(nc)
        while (!(m contains rib)) {
          rib = r.nextInt(nc)
        }
        val xs2 = m(rib)
        val bs   = xs2.foldLeft(0.0d) { (acc, el) =>
          acc + tfidf.cosineSimilarity(seed, el._2)
        }

        println(e._1, as, bs, (bs - as) / Math.max(as, bs))
        (as, bs, (bs - as) / Math.max(as, bs))
      }
    }

    println(silhouette)

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
