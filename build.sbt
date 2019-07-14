name := "parquet2hive"
organization := "org.vollgaz.spark"
scalaVersion := "2.11.12"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-encoding", "UTF-8")

libraryDependencies ++= {
    val sparkVersion = "2.2.0"
    Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
    )
}

// sbt-assembly
mainClass in(Compile, run) := Some("org.vollgaz.spark.main")
assemblyOption in assembly := (assemblyOption in assembly).value
    .copy(includeScala = false)


import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    pushChanges
)