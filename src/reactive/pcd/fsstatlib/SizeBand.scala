package pcd.fsstatlib


/*
  Example: upperSize = 20, boundedBandCount = 4
    --> [0, 5], [6, 10], [11, 15], [16, 20], [21, ...]
 */


enum SizeBand:

  case Bounded(lower: Long, upper: Long)

  case BoundedBelow(lower: Long)


case class BandsSpec(upperSize: Long, boundedBandCount: Int):

  private val bandWidth = upperSize.toDouble / boundedBandCount.toDouble

  def totalBandCount: Int =
    boundedBandCount + 1

  def findBandIndex(size: Long): Int =
    if size == 0 then 0
    else if size > upperSize then boundedBandCount
    else (size / bandWidth).intCeil - 1

  def makeBands: Seq[SizeBand] =
    (0 to totalBandCount).map(bandAtIndex)

  private def bandAtIndex(i: Int): SizeBand =
    if i == boundedBandCount then
      return SizeBand.BoundedBelow(upperSize + 1)
    val lower = if i == 0 then 0 else (i * bandWidth).intCeil + 1
    val upper = ((i + 1) * bandWidth).intCeil
    SizeBand.Bounded(lower, upper)

