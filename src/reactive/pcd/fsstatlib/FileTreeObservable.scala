package pcd.fsstatlib

import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers

import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes


object FileTreeObservable:

  def apply(startingDirectory: Path): Observable[(Path, BasicFileAttributes)] =
    Observable
      .defer(() => walk(startingDirectory))
      .subscribeOn(Schedulers.io())

  private def walk(path: Path): Observable[(Path, BasicFileAttributes)] =
    val attributes = readAttributes(path)
    if attributes.isDirectory then
      val stream = Files.newDirectoryStream(path)
      Observable
        .fromIterable(stream)
        .concatMap(path =>
          Observable
            .defer(() => walk(path))
            .onErrorComplete() // Just skip problematic files/subtrees
        )
        .doAfterTerminate(() => stream.close())
    else if attributes.isRegularFile then
      Observable.just((path, attributes))
    else
      Observable.empty()

  private def readAttributes(path: Path): BasicFileAttributes =
    Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)

