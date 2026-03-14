//name of the package
name := "scala"
//package version
version := "1.0"
//version of Scala
scalaVersion := "2.12.10"
resolvers += "Amazon Deequ" at "https://aws.oss.sonatype.org/content/repositories/releases/"
// spark library dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql"  % "3.1.1",
  "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1",
  "io.dataflint" %% "spark" % "0.8.6"
)
