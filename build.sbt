name := "MyTwitterSpark"

version := "0.1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.0",
"org.apache.spark" %% "spark-streaming" % "2.4.0",
"org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.2.0",
"org.apache.spark" %% "spark-streaming-twitter" % "2.4.0")
