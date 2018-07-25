name := "Fun_Mit_Twitter"

version := "0.1"

scalaVersion := "2.11.8"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.6"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.6"