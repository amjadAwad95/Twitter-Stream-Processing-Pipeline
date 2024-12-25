ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Data-Processing"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
libraryDependencies +="org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3"
libraryDependencies += "org.tensorflow" % "tensorflow-core-platform" % "0.3.3"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.5.7"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.5.7" classifier "models"





