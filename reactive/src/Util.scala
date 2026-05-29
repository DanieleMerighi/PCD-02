package pcd.fsstatlib


extension [A](s: Seq[A])
  def mapAt(i: Int, mapper: A => A): Seq[A] =
    val old = s(i)
    s.updated(i, mapper(old))


extension (d: Double)
  def intCeil: Int =
    d.ceil.toInt
    
