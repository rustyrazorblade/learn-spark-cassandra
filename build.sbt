name := "untitled6"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.2"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0" withSources() withJavadoc()
