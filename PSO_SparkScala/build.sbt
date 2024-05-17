ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.8.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % "2.8.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "1.0.1"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided

//libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.8"

//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test
//libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.16"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % "test"

//resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"


scalacOptions += "-Xasync"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0"
)

libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "1.0.0-M1"


lazy val root = (project in file("."))
  .settings(
    name := "red_neuronal_DSPSO"
  )