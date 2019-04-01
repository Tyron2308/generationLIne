import AssemblyKeys._
import sbt.Keys.libraryDependencies
import sbtassembly.Plugin._
assemblySettings

version := "0.1"
scalaVersion := "2.11.8"
name := "generateur_ligne"

/*lazy val commonSettings = Seq(*/
  /*organization := "com.lbp.ingestion",*/
  /*version := "0.1.0-SNAPSHOT",*/
  /*scalaVersion := "2.11.8"*/
/*)*/

/*lazy val generatorLine = (project in file(".")).settings(*/
    /*commonSettings,*/
    /*name := "generateur_ligne",*/
    /*mainClass in (Compile, run) := Some("GenerateOBJ"),*/
/*//    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0"*/
    /*//libraryDependencies += "org.apache.spark" %% "spark-hive"       % "2.0.0" % "test"*/
/*).dependsOn(moteurIngestion)*/

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.1.0",
                            "org.apache.spark" % "spark-sql_2.11" % "2.1.0" )
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

