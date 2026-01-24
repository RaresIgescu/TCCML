name := "DeltaOCC_Thesis_Benchmark"

version := "1.0"

scalaVersion := "2.12.18" // Spark 3.x works best with Scala 2.12 or 2.13

val sparkVersion = "3.5.0"
val deltaVersion = "3.0.0" 

libraryDependencies ++= Seq(
  // Core Spark Engine
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  
  // Delta Lake Support
  "io.delta" %% "delta-spark" % deltaVersion,
  
  // Azure Connectivity (Matches your hadoop-azure-datalake jars)
  "org.apache.hadoop" % "hadoop-azure" % "3.3.4",
  "org.apache.hadoop" % "hadoop-azure-datalake" % "3.3.4"
)

// Fix for some common logging warnings
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.7"

fork := true

javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)