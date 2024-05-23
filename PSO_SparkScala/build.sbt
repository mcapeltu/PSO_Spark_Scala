ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.8.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % "2.8.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "1.0.1"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" excludeAll (
  ExclusionRule("org.scala-lang", "scala-library"),
  ExclusionRule("org.scala-lang.modules", "scala-collection-compat_2.11"),
  ExclusionRule("com.typesafe.akka", "akka-actor-typed_2.12")
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)
scalacOptions += "-Xasync"
///////////////////////////////
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x => MergeStrategy.first
}
//resolvers += "Rapids.ai" at "https://repo.rapids.ai/artifactory/spark-rapids"
///////////////////////////////////////////////////////////////////////
libraryDependencies += "com.nvidia" %% "rapids-4-spark" % "23.10.0"
//////////////////////////////////////////////////////////////////
lazy val root = (project in file("."))
  .settings(
    name := "red_neuronal_DSPSO"
  )