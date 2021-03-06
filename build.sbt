name := "SparkEtlSbt"

version := "1.0"

scalaVersion := "2.12.11"
mainClass in Compile := Some("com.spark.etl.SparkEtlMain")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.0.0",
  "org.apache.spark" % "spark-sql_2.12" % "3.0.0"
)

assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
