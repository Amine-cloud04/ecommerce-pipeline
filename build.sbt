ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "ecommerce-pipeline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql"  % "3.5.1"
    ),
    // needed so spark doesn't complain about logging
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36"
  )
