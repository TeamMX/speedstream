name := "speedstream"
version := "1.0"
// we are using 2.11.12 as spark needs 2.11.x and scala metals supports 2.11.12 at the earliest
// TODO: upgrade to 2.12.x as soon as spark supports it; scala metals 2.11.x is depreciated and may be discontinued
scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
