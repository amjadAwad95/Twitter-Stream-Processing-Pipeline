ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "KafkaProducer"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.8.1"

dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-4"