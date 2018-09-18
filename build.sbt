
organization := "com.gitlab.leafty"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.11.12"
name := "test-hudi"

lazy val root = project in file(".")

resolvers += "Spring Plugins" at "https://repo.spring.io/plugins-release/"
resolvers += "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Cloudera lib snapshots" at "http://repository.cloudera.com/cloudera/libs-snapshot"
resolvers += "Cloudera Packages" at "http://repository.cloudera.com/cloudera/cdh-releases-rcs"
resolvers += "Cloudera Repos" at "http://repository.cloudera.com/cloudera/cloudera-repos"
resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

//required for hive queries to be run from spark
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" exclude("org.apache.commons", "lang3")

libraryDependencies += "com.uber.hoodie" % "hoodie-spark" % "0.4.3-y-SNAPSHOT"

libraryDependencies += "com.uber.hoodie" % "hoodie-hive" % "0.4.3-y-SNAPSHOT"

libraryDependencies += "mrpowers" % "spark-daria" % "2.3.0_0.18.0"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.2.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.16"