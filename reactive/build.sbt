ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "3.8.3"

ThisBuild / libraryDependencies += "io.reactivex.rxjava3" % "rxjava" % "3.1.12"

Compile / scalaSource := baseDirectory.value / "src"

lazy val root = (project in file("."))
  .settings(
    name := "fsstatlib-reactive",
    idePackagePrefix := Some("pcd.fsstatlib"),
  )
