name := "SparkEtlSbt"

version := "0.1"

scalaVersion := "2.12.11"
mainClass in Compile := Some("com.spark.etl.SparkEtlMain")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.0.0",
  "org.apache.spark" % "spark-sql_2.12" % "3.0.0",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.2.4",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "8.4.0.jre8"
)

assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
