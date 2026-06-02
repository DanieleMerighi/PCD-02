package pcd.fsstatlib

import io.reactivex.rxjava3.core.{Emitter, Flowable}
import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.CompletableSubject

import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.IterableHasAsJava


object FileTreeFlowable:

  def apply(startingDirectory: Path,
            stopper: Option[CompletableSubject] = None,
            parallelism: Int = 1): Flowable[(Path, BasicFileAttributes)] =

    val iterator = try
      ConcurrentFileTreeIterator(startingDirectory)
    catch
      case e: IOException =>
        stopper.onComplete()
        return Flowable.error(e)

    def generator(emitter: Emitter[(Path, BasicFileAttributes)]): Unit =
      if stopper.hasComplete then
        emitter.onComplete()
        return
      val next = iterator.tryNext
      if next.nonEmpty then
        emitter.onNext(next.get)
        return
      emitter.onComplete()
      stopper.onComplete()

    val sources = (0 until parallelism).map: _ =>
      Flowable
        .generate(generator)
        .subscribeOn(Schedulers.io())
    Flowable
      .merge(sources.asJava)
      .doAfterTerminate(() => iterator.close())


class ConcurrentFileTreeIterator(startingDirectory: Path):

  private val queue = ConcurrentLinkedQueue[DirectoryNode]()
  checkStartingDirectory()
  queue.offer(DirectoryNode(startingDirectory))

  private case class DirectoryNode(path: Path):
    private val stream = Files.newDirectoryStream(path)
    private val iterator = stream.iterator()
    def close(): Unit = synchronized:
      stream.close()
    def tryNext: Option[Path] = synchronized:
      Option.when(iterator.hasNext)(iterator.next())

  def tryNext: Option[(Path, BasicFileAttributes)] =
    while true do
      val node = queue.peek()
      if node == null then
        return Option.empty
      val next = tryNext(node)
      if next.nonEmpty then
        return Some(next.get)
    null // unreachable

  private def tryNext(node: DirectoryNode): Option[(Path, BasicFileAttributes)] =
    while true do
      try
        val path = node.tryNext
        if path.isEmpty then
          queue.remove(node)
          node.close()
          return None
        else
          val attributes = readAttributes(path.get)
          if attributes.isDirectory then
            queue.offer(DirectoryNode(path.get))
            return None
          else if attributes.isRegularFile then
            return Some((path.get, attributes))
      catch
        case _: IOException | _: DirectoryIteratorException =>
    null // unreachable

  private def checkStartingDirectory(): Unit =
    if Files.notExists(startingDirectory, LinkOption.NOFOLLOW_LINKS) then
      throw NoSuchFileException(startingDirectory.toString)
    if !Files.isDirectory(startingDirectory, LinkOption.NOFOLLOW_LINKS) then
      throw NotDirectoryException(startingDirectory.toString)

  private def readAttributes(path: Path): BasicFileAttributes =
    Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)

  def close(): Unit =
    queue.forEach(_.close())

