name := "spark-snowflake-test"

version := "0.1"

//scalaVersion := "2.12.3"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"


scalaVersion := "2.12.12"
val sparkVersion = "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "net.snowflake" % "spark-snowflake_2.12" % "2.8.2-spark_3.0"
