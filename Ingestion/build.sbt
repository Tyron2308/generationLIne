import AssemblyKeys._

name := "Ingestion"

version := "1.0.2"

scalaVersion := "2.11.7"
val sparkVersion = "2.1.0"
val sparkTestingBaseVersion = "0.7.4"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

libraryDependencies += "org.scalamock" %% "scalamock" % "4.1.0"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0"
libraryDependencies += "info.cukes" %% "cucumber-scala" % "1.2.5"
libraryDependencies += "org.mockito" % "mockito-all" % "1.8.5"
libraryDependencies += "info.cukes" % "cucumber-junit" % "1.2.4"
libraryDependencies += "junit" % "junit" % "4.12"
libraryDependencies += "commons-lang" % "commons-lang" % "2.6"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_$sparkTestingBaseVersion" % "test"


assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

mergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*)    => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*)     => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*)  => MergeStrategy.last
  case PathList("org", "apache", xs @ _*)       => MergeStrategy.last
  case PathList("com", "google", xs @ _*)       => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}
