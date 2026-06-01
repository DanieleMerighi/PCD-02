package pcd.fsstatlib

import io.reactivex.rxjava3.subjects.CompletableSubject


extension [A](s: Seq[A])
  def mapAt(i: Int, mapper: A => A): Seq[A] =
    val old = s(i)
    s.updated(i, mapper(old))


extension (d: Double)
  def intCeil: Int =
    d.ceil.toInt


extension (maybeSubject: Option[CompletableSubject])
  def onComplete(): Unit =
    maybeSubject.foreach(_.onComplete())
  def hasComplete: Boolean =
    maybeSubject.exists(_.hasComplete)

