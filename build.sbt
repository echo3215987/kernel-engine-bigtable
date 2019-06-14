name := "rcaKernelEngine"

version := "0.1"

scalaVersion := "2.12.8"
//scalaVersion := "2.11.8"

mainClass in assembly := Some("com.foxconn.iisd.bd.rca.KernelEngineV1")

assemblyJarName in assembly := { s"${name.value}.jar" }

val sparkVersion = "2.4.0"
//val hadoopVersion = "2.7.2"
//val awsVersion = "1.7.4"
val hadoopVersion = "3.1.1"
val awsVersion = "1.11.271"

resolvers ++= Seq(
    "Maven2" at "http://central.maven.org/maven2/"
)

//enablePlugins(JavaAppPackaging)

dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
    "org.glassfish.jersey.core" % "jersey-server" % "2.22.2",
    "org.glassfish.jersey.core" % "jersey-client" % "2.22.2",
    "org.glassfish.jersey.core" % "jersey-common" % "2.22.2",
    "org.glassfish.jersey.containers" % "jersey-container-servlet" % "2.22.2",
    "org.glassfish.jersey.containers" % "jersey-container-servlet-core" % "2.22.2",
    "org.glassfish.jersey.ext" % "jersey-bean-validation" % "2.22.2"
)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion ,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    //    "org.apache.spark" %% "spark-kubernetes" % sparkVersion,
    //    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    //    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "com.typesafe.slick" %% "slick" % "3.2.3",
    "com.typesafe.slick" %% "slick-codegen" % "3.2.3",
    "org.postgresql" % "postgresql" % "42.1.1",
    //    "org.mariadb.jdbc" % "mariadb-java-client" % "2.3.0",
    "mysql" % "mysql-connector-java" % "8.0.14",
    "org.yaml" % "snakeyaml" % "1.23",
    //    "io.minio" % "minio" % "6.0.2",
    "com.amazonaws" % "aws-java-sdk" % awsVersion,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
    //unarchive xz file
    "com.databricks" %% "spark-xml" % "0.5.0",
    "org.apache.commons" % "commons-compress" % "1.18"

)

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) =>
        (xs map {
            _.toLowerCase
        }) match {
            case ("manifest.mf" :: Nil) | ("index.list" :: Nil) |
                 ("dependencies" :: Nil) =>
                MergeStrategy.discard
            case ps@(x :: xs)
                if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
                MergeStrategy.discard
            case "plexus" :: xs =>
                MergeStrategy.discard
            case "services" :: xs =>
                MergeStrategy.filterDistinctLines
            case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
                MergeStrategy.filterDistinctLines
            case _ => MergeStrategy.discard
        }
    case PathList(ps @ _*)
        if ps contains "foxconn" => MergeStrategy.first
    case PathList(ps @ _*)
        if ps contains "databricks" => MergeStrategy.first
    case PathList("org", "apache", "commons", "compress", ps @ _*) => MergeStrategy.first
    case x =>
        MergeStrategy.discard
        //MergeStrategy.first
}

//artifact in (Compile, packageBin) := {
//  val previous: Artifact = (artifact in (Compile, packageBin)).value
//  previous.withType("bundle")
//}
//
//artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
//    module.name + "." + artifact.name + "." + module.revision + "." + artifact.classifier
//}