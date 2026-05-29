package pcd.fsstatlib

import io.reactivex.rxjava3.core.{BackpressureStrategy, Flowable, FlowableEmitter}
import io.reactivex.rxjava3.schedulers.Schedulers

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}


object FileTreeFlowable:

  def apply(startingDirectory: Path): Flowable[(Path, BasicFileAttributes)] =

    def source(emitter: FlowableEmitter[(Path, BasicFileAttributes)]): Unit =
      Files.walkFileTree(startingDirectory, new SimpleFileVisitor[Path]:
        override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult =
          if attrs.isRegularFile then
            emitter.onNext((path, attrs))
          FileVisitResult.CONTINUE
        override def visitFileFailed(path: Path, e: IOException): FileVisitResult =
          if path == startingDirectory then
            emitter.onError(e)
          FileVisitResult.SKIP_SUBTREE
      )
      emitter.onComplete()
    
    Flowable
      .create(source, BackpressureStrategy.MISSING) // handling backpressure is up to the caller
      .subscribeOn(Schedulers.io())

