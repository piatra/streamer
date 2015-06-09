package Kmeans

import kmeans.KMeans
import org.scalatest._

class ExampleSpec extends FunSuite {

  test("should generate the correct number of clusters") {
    val kmeans = new KMeans(Vector(
      Vector("Ana", "are", "mere"),
      Vector("Ana", "are", "mere"),
      Vector("Ana", "nu", "are", "mere"),
      Vector("Ana", "are", "mere"),
      Vector("Andrei", "are", "pere"),
      Vector("Andrei", "are", "pere"),
      Vector("Andrei", "are", "pere"),
      Vector("Gigi", "are", "pere")))

    assert(kmeans.clusterIterations(1).toList.distinct.size === 2)
  }
}


