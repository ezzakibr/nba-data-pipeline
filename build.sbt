name := "NBA"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.1.8",
  "com.lihaoyi" %% "ujson" % "0.7.1",
  "com.lihaoyi" %% "os-lib" % "0.7.8",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.spark" %% "spark-core" % "3.3.1" % "provided",
  "com.typesafe.play" %% "play-json" % "2.9.2",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-log4j12" % "1.7.32",
  "mysql" % "mysql-connector-java" % "8.0.28"
)