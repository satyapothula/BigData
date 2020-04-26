name := "bixi"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"

resolvers += " Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"