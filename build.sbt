import ReleaseTransformations._

name := "aws-kinesis-scala"

lazy val commonSettings = Seq(
  organization := "jp.co.bizreach",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq(scalaVersion.value, "2.11.12"),
  scalacOptions ++= Seq("-feature", "-deprecation"),
  publishTo := sonatypePublishTo.value,
  publishArtifact in Test := false,
  publishMavenStyle := true,
  homepage := Some(url(s"https://github.com/bizreach/aws-kinesis-scala")),
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  pomExtra := (
    <scm>
      <url>https://github.com/bizreach/aws-kinesis-scala</url>
      <connection>scm:git:https://github.com/bizreach/aws-kinesis-scala.git</connection>
    </scm>
    <developers>
      <developer>
        <id>takezoe</id>
        <name>Naoki Takezoe</name>
        <email>naoki.takezoe_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
      <developer>
        <id>shimamoto</id>
        <name>Takako Shimamoto</name>
        <email>takako.shimamoto_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
    </developers>
  ),
  sonatypeProfileName := organization.value,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseTagName := (version in ThisBuild).value,
  releaseCrossBuild := true,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    releaseStepCommand("sonatypeRelease"),
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val root = (project in file("."))
  .aggregate(core, spark)
  .settings(commonSettings: _*)
  .settings(
    packagedArtifacts := Map.empty
  )

val sdkVersion = "1.11.550"
val sparkVersion = "2.4.3"

lazy val core = project
  .settings(commonSettings: _*)
  .settings(
    name := "aws-kinesis-scala",
    libraryDependencies ++= Seq(
      "com.amazonaws" %  "aws-java-sdk-kinesis" % sdkVersion,
      "org.slf4j"     %  "slf4j-api"            % "1.7.26",
      "org.scalatest" %% "scalatest"            % "3.0.7" % "test"
    )
  )

lazy val spark = project
  .settings(commonSettings: _*)
  .settings(
    name := "aws-kinesis-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"  % sparkVersion % "provided",
      "com.amazonaws"    %  "aws-java-sdk-sts" % sdkVersion
    )
  ).dependsOn(core % "compile->compile;test->test")

