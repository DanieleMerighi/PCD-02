package pcd.fsstatlib

import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.CompletableSubject

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{DirectoryIteratorException, DirectoryStream, Files, LinkOption, NoSuchFileException, NotDirectoryException, Path}


object FileTreeFlowable:

  def apply(startingDirectory: Path, stopper: Option[CompletableSubject] = None): Flowable[(Path, BasicFileAttributes)] =
    val source: Flowable[(Path, BasicFileAttributes)] =
      startingDirectoryError(startingDirectory) match
        case Some(e) => Flowable.error(e)
        case None    => walk(startingDirectory)
    val files = stopper match
      case Some(s) => source.takeUntil(s.toFlowable[(Path, BasicFileAttributes)])
      case None    => source
    files
      .doOnTerminate(() => stopper.onComplete())
      .subscribeOn(Schedulers.io())

  private def startingDirectoryError(directory: Path): Option[IOException] =
    if Files.notExists(directory, LinkOption.NOFOLLOW_LINKS) then
      Some(NoSuchFileException(directory.toString))
    else if !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) then
      Some(NotDirectoryException(directory.toString))
    else None

  private def visit(entry: Path): Flowable[(Path, BasicFileAttributes)] =
    try
      if Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS) then
        Flowable.defer(() => walk(entry)).onErrorComplete()
      else
        val attr = Files.readAttributes(entry, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
        if attr.isRegularFile then Flowable.just((entry, attr))
        else Flowable.empty()
    catch
      case _ => Flowable.empty()

  private def walk(path: Path): Flowable[(Path, BasicFileAttributes)] =
    Flowable.using(
      () => Files.newDirectoryStream(path),
      (stream: DirectoryStream[Path]) => Flowable.fromIterable(stream).concatMap(visit),
      (stream: DirectoryStream[Path]) => stream.close()
    )
