ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

//ThisBuild / scalaVersion := "2.13.11"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.9.0-M1"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % "2.9.0-M1"

libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "1.0.1"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" excludeAll (
  ExclusionRule("org.scala-lang", "scala-library"),
  ExclusionRule("org.scala-lang.modules", "scala-collection-compat_2.12"),
  ExclusionRule("com.typesafe.akka", "akka-actor-typed_2.12")
)

//libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.8"

//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test
//libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.16"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % "test"

//resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"


scalacOptions += "-Xasync"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

//resolvers += "Rapids.ai" at "https://repo.rapids.ai/artifactory/spark-rapids"

libraryDependencies += "com.nvidia" %% "rapids-4-spark" % "23.10.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "1.0.0-M1"

lazy val root = (project in file("."))
  .settings(
    name := "red_neuronal_fumadores"
  )
