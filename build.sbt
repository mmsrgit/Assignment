name := "LixarAssignment"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.3.0"
