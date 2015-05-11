import AssemblyKeys._

assemblySettings

mainClass in assembly := Some("App")

jarName in assembly := "streamer.jar"

fork in Compile := true

javaOptions in Compile := Seq("-Xmx2G")