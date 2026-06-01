package pcd.fsstatlib

import io.reactivex.rxjava3.core.{Emitter, Flowable}
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.CompletableSubject

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{DirectoryIteratorException, Files, LinkOption, NoSuchFileException, NotDirectoryException, Path}
import scala.collection.mutable


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

  private class DirectoryNode(path: Path) extends Iterator[Path], AutoCloseable:
    private val stream = Files.newDirectoryStream(path)
    private val iter = stream.iterator()
    export iter._, stream.close

  checkStartingDirectory()
  private val stack: mutable.Stack[DirectoryNode] =
    mutable.Stack(DirectoryNode(startingDirectory))
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
    while iter.hasNext() do
      val path = iter.next()
      try
        if isDirectory(path) then
          stack.push(DirectoryNode(path))
          return false
        else
          val attr = readAttributes(path)
          if attr.isRegularFile then
            cachedNext = Some(path, attr)
            return true
      catch
        case _: IOException | _: DirectoryIteratorException =>
    stack.pop().close()
    false

  private def checkStartingDirectory(): Unit =
    if Files.notExists(startingDirectory, LinkOption.NOFOLLOW_LINKS) then
      throw NoSuchFileException(startingDirectory.toString)
    if !Files.isDirectory(startingDirectory, LinkOption.NOFOLLOW_LINKS) then
      throw NotDirectoryException(startingDirectory.toString)

  private def isDirectory(path: Path): Boolean =
    Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)

  private def readAttributes(path: Path): BasicFileAttributes =
    Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)

