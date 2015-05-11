name := "HelloWorld"

version := "1.0"

scalaVersion := "2.10.5"

fork in Compile := true

javaOptions in Compile := Seq("-Xmx2G")

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.2"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.1"

libraryDependencies += "com.twitter" % "finagle-http_2.10" % "6.25.0"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.6"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.0"