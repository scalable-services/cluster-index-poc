ThisBuild / version := "0.10"

ThisBuild / scalaVersion := "2.13.11"

ThisBuild / organization := "services.scalable"

lazy val root = (project in file("."))
  .settings(
    name := "cluster-index-poc"
  )

val jacksonVersion = "2.11.4"
lazy val akkaVersion = "2.7.0"
lazy val akkaHttpVersion = "10.5.0-M1"
val pulsarVersion = "3.0.0"

ThisBuild / libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",

  "services.scalable" %% "index" % "0.30",
  "services.scalable" %% "index" % "0.30" % "protobuf",
  "com.typesafe.akka" %% "akka-http2-support" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.typesafe.akka" %% "akka-pki" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,

  "com.typesafe.akka" %% "akka-stream-kafka" % "3.0.1",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  //"io.vertx" % "vertx-kafka-client" % "4.4.1",

  /*"org.apache.pulsar" % "pulsar-client" % pulsarVersion,
  "org.apache.pulsar" % "pulsar-client-admin" % pulsarVersion*/
)

enablePlugins(AkkaGrpcPlugin)