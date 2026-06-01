package pcd.fsstatlib

import io.reactivex.rxjava3.core.{Emitter, Flowable}
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.CompletableSubject

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, LinkOption, NoSuchFileException, NotDirectoryException, Path}
import scala.collection.mutable
import scala.jdk.StreamConverters.StreamHasToScala


object FileTreeFlowable:

  def apply(startingDirectory: Path, stopper: Option[CompletableSubject] = None): Flowable[(Path, BasicFileAttributes)] =

    val iterator = try
        FileTreeIterator(startingDirectory)
    catch
      case e: IOException =>
        stopper.onComplete()
        return Flowable.error(e)

    def generator(emitter: Emitter[(Path, BasicFileAttributes)]): Unit =
      if !stopper.hasComplete && iterator.hasNext then
        emitter.onNext(iterator.next())
      else
        emitter.onComplete()
        stopper.onComplete()

    Flowable
      .generate(generator)
      .subscribeOn(Schedulers.io())


class FileTreeIterator(startingDirectory: Path) extends Iterator[(Path, BasicFileAttributes)]:

  checkStartingDirectory()
  private val stack: mutable.Stack[Iterator[Path]] =
    mutable.Stack(iterator(startingDirectory))
  private var cachedNext: Option[(Path, BasicFileAttributes)] = None

  override def next(): (Path, BasicFileAttributes) =
    if cachedNext.isEmpty then
      findNext()
    val next = cachedNext.get
    cachedNext = None
    next

  override def hasNext: Boolean =
    if cachedNext.isEmpty then
      findNext()
    cachedNext.nonEmpty

  private def findNext(): Unit =
    while stack.nonEmpty do
      if digNext() then
        return

  private def digNext(): Boolean =
    val iter = stack.top
    while iter.hasNext do
      val path = iter.next()
      try
        if isDirectory(path) then
          stack.push(iterator(path))
          return false
        else
          val attr = readAttributes(path)
          if attr.isRegularFile then
            cachedNext = Some(path, attr)
            return true
      catch
        case _: IOException =>
    stack.pop()
    false

  private def checkStartingDirectory(): Unit =
    if Files.notExists(startingDirectory, LinkOption.NOFOLLOW_LINKS) then
      throw NoSuchFileException(startingDirectory.toString)
    if !Files.isDirectory(startingDirectory, LinkOption.NOFOLLOW_LINKS) then
      throw NotDirectoryException(startingDirectory.toString)

  private def iterator(path: Path): Iterator[Path] =
    Files.list(path).toScala(Iterator)

  private def isDirectory(path: Path): Boolean =
    Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)

  private def readAttributes(path: Path): BasicFileAttributes =
    Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)

