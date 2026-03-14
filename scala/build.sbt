//name of the package
name := "scala"
//version of our package
version := "1.0"
//version of Scala
scalaVersion := "2.12.10"
resolvers += "Amazon Deequ" at "https://aws.oss.sonatype.org/content/repositories/releases/"
// spark library dependencies 
// change this to 3.0.0 when released
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql"  % "3.1.1",
  "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1",
  "io.dataflint" %% "spark" % "0.8.6"
)
