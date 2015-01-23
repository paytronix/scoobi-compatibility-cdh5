import SonatypeKeys._

organization := "com.nicta"

name := "scoobi-compatibility-cdh5"

version := "1.0.3-px-1"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.4", "2.11.2")

resolvers += "Cloudera Maven Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-client" % "2.5.0-cdh5.2.1" exclude("asm", "asm"),
                            "org.apache.avro"   % "avro-mapred"   % "1.7.6-cdh5.2.1")

// publishTo <<= version { v: String =>
//     val nexus = "https://oss.sonatype.org/"
//     if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
//     else                             Some("staging"   at nexus + "service/local/staging/deploy/maven2")
//   }

publishTo <<= version { v: String =>
  val nexus = "https://nexus.corp.paytronix.com/nexus/"
  if (v.trim.endsWith("SNAPSHOT")) Some("paytronix-snapshots" at (nexus + "content/repositories/snapshots"))
  else                             Some("paytronix-releases" at (nexus + "content/repositories/releases"))
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { x => false }

pomExtra := (
    <url>http://nicta.github.io/scoobi</url>
      <licenses>
        <license>
          <name>Apache 2.0</name>
          <url>http://www.opensource.org/licenses/Apache-2.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>http://github.com/NICTA/scoobi</url>
        <connection>scm:http:http://NICTA@github.com/NICTA/scoobi.git</connection>
      </scm>
      <developers>
        <developer>
          <id>etorreborre</id>
          <name>Eric Torreborre</name>
          <url>http://etorreborre.blogspot.com/</url>
        </developer>
      </developers>
    )

// credentials := Seq(Credentials(Path.userHome / ".sbt" / "scoobi.credentials"))

// sonatypeSettings
