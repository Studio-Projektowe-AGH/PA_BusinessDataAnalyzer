name := """Statistics_Service"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayJava)

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  javaWs
)

libraryDependencies += "org.mongodb.morphia" % "morphia" % "0.111"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "2.11.0"

libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.5.0"

libraryDependencies += "com.google.guava" % "guava" % "12.0"

libraryDependencies += "com.restfb" % "restfb" % "1.10.1"

libraryDependencies += "org.jboss.resteasy" % "resteasy-jaxrs" % "3.0.11.Final"

libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13"

libraryDependencies += "org.jboss.resteasy" % "resteasy-jackson2-provider" % "3.0.11.Final"

libraryDependencies += "com.nimbusds" % "nimbus-jose-jwt" % "2.22.1"

libraryDependencies += "joda-time" % "joda-time" % "2.8.1"

libraryDependencies += "org.json" % "json" % "20141113"



fork in run := true