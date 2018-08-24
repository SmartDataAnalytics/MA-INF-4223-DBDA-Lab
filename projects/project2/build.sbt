name := "carl-test"

version := "0.0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

mainClass in (Compile, run) := Some("com.kg.carl.Preprocessor")  

mainClass in (Compile, packageBin) := Some("com.kg.carl.Preprocessor") 

resolvers += "Akka Repository" at "http://repo.akka.io/releases/" 

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

mainClass in Compile := Some("com.kg.carl.Preprocessor")
