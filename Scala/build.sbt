name := "SparkCassandra"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
    "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
    "mvn" at "https://mvnrepository.com/artifact/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0"
)