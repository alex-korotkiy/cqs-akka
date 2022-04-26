name := "cqs-akka"

version := "0.1"

scalaVersion := "2.13.3"
lazy val akkaVersion       = "2.6.14"
lazy val leveldbVersion    = "0.7"
lazy val leveldbjniVersion = "1.8"
lazy val postgresVersion   = "42.2.2"
lazy val cassandraVersion  = "1.0.5"
lazy val json4sVersion     = "3.2.11"
lazy val protobufVersion   = "3.6.1"
lazy val scalikejdbc       = "3.5.0"
lazy val alpakkaSlickVersion  = "3.0.4"

scalacOptions += "-deprecation"
resolvers += Resolver.bintrayRepo("akka", "snapshots")

libraryDependencies ++= Seq(

  "org.postgresql"     % "postgresql"             % postgresVersion,
  "org.scalikejdbc"   %% "scalikejdbc"            % scalikejdbc,

  "com.typesafe.akka" %% "akka-persistence-cassandra"          % cassandraVersion,
  "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % cassandraVersion,

  "com.typesafe.akka" %% "akka-actor-typed"       % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,

  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % alpakkaSlickVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-coordination" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,


  //  local levelDB stores
  "org.iq80.leveldb"          % "leveldb"        % leveldbVersion,
  "org.fusesource.leveldbjni" % "leveldbjni-all" % leveldbjniVersion

)

libraryDependencies += "com.typesafe"   % "config"          % "1.4.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"