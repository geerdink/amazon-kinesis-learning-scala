import sbt.Keys.libraryDependencies

name := "amazon-kinesis-learning-scala"

version := "1.0"

scalaVersion := "2.11.8"

val slf4jV = "1.7.22" // Our logging framework
val logbackV = "1.2.3" // Our logging implementation
val json4sV = "3.2.11"
val scalatestV = "3.0.4"

libraryDependencies ++= Seq(
  "org.json4s"        %% "json4s-native"        % json4sV,
  "org.json4s"        %% "json4s-jackson"       % json4sV,
  "org.slf4j"         %  "slf4j-api"            % slf4jV,
  "ch.qos.logback"    %  "logback-classic"      % logbackV,
  "ch.qos.logback"    %  "logback-core"         % logbackV,
  "org.scalactic"     %% "scalactic"            % scalatestV,
  "org.scalatest"     %% "scalatest"            % scalatestV % "test"
)

libraryDependencies ++= Seq(
  "com.amazonaws"     %  "aws-java-sdk-kinesis"  % "1.11.221",
  "com.amazonaws"     %  "amazon-kinesis-client" % "1.8.7"
)

fork in run := true

