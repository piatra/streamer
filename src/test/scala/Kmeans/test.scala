package Kmeans

import kmeans.KMeans
import org.scalatest._

class ExampleSpec extends FlatSpec with Matchers {

  "assignToClosest" should "return the closest centroid" in {
    var kmeans = new KMeans(List(
      List("Ana are mere"),
      List("Andrei are pere"),
      List("Gigi are pere")))
  }
}


